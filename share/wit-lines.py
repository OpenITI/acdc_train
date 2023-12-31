import argparse, re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, length, lit, struct, translate, udf
import pyspark.sql.functions as f

# length of the maximum alignment gap
def maxGap(s):
    res = 0
    cur = 0
    for c in s:
        if c == '-':
            cur += 1
        elif cur > 0:
            if cur > res:
                res = cur
            cur = 0
    if cur > res:
        res = cur
    return res

def fixHyphen(src, dst):
    if len(src) >= 3 and len(dst) >= 3 and dst.endswith('\u2010\n') and dst[-3] != '-' and src[-3] != '-' and src.endswith('--'):
        src = src[0:(len(src)-2)] + '\u2010\n'
    return src

def fixCase(src, dst):
    res = list(src)
    i = 0
    while i < (len(res)-1):
        if res[i] != dst[i] and res[i].lower() == dst[i].lower() and res[i+1] == dst[i+1] and (i == 0 or res[i-1] == dst[i-1]):
            res[i] = dst[i]
        i += 1
    return ''.join(res)

def digitMatch(src, dst):
    "Intersection-over-union of digits"
    union = 0
    inter = 0
    for i in range(len(src)):
        if src[i].isdigit():
            union += 1
            if src[i] == dst[i]:
                inter += 1
        elif dst[i].isdigit():
            union += 1
    if union > 0:
        return inter / union
    else:
        return 1.0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Witness lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--min-line', type=int, default=5,
                         help='Minimum length of line', metavar='N')
    parser.add_argument('--fix-case', action='store_true',
                        help='Match case in destination.')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Witness lines').getOrCreate()

    max_gap = udf(lambda s: maxGap(s), 'int')
    fix_hyphen = udf(lambda src, dst: fixHyphen(src, dst))
    fix_case = udf(lambda src, dst: fixCase(src, dst) if config.fix_case else src)
    digit_match = udf(lambda src, dst: digitMatch(src, dst), 'double')
    sstrip = udf(lambda s: s.strip())

    raw = spark.read.json(config.inputPath)
    if 'lineIDs' not in raw.columns:
        r = Row(start=0, length=0, id='')
        raw = raw.withColumn('lineIDs', f.array(struct(lit(0).alias('start'), lit(0).alias('length'), lit('').alias('id'))))
    
    raw.select('id', 'lineIDs', col('pages')[0].alias('page'), explode('lines').alias('line')
        ).filter(col('line.wits').isNotNull()
        ).select(col('page.id').alias('img'), 'id', 'lineIDs', col('page.regions'),
                 col('page.width'), col('page.height'),
                 col('line.begin'), length(sstrip('line.text')).alias('length'),
                 col('line.text').alias('dstText'),
                 col('line.wits')[0]['id'].alias('src'),
                 col('line.wits')[0]['matches'].alias('matches'),
                 translate(col('line.wits')[0]['alg'], '\n', ' ').alias('srcAlg'),
                 col('line.wits')[0]['alg2'].alias('dstAlg')
        ).withColumn('dstLength', length(sstrip(translate('dstAlg', '-', '')))
        ).filter(col('dstLength') >= config.min_line
        ).withColumn('srcAlg', fix_hyphen('srcAlg', 'dstAlg')
        ).withColumn('srcOrig', col('srcAlg')
        ).withColumn('srcAlg', fix_case('srcAlg', 'dstAlg')
        ).withColumn('srcText', translate('srcAlg', '\n\u2010-', ' -')                     
        ).withColumn('matchRate',
                     col('matches') / f.greatest(length('dstText'), length('srcText'))
        ).withColumn('maxGap', f.greatest(max_gap('srcAlg'), max_gap('dstAlg'))
        ).withColumn('leadGap', f.greatest(length(f.regexp_extract('dstAlg', r'^\s*(\-+)', 1)),
                                           length(f.regexp_extract('srcAlg', r'^\s*(\-+)', 1)))
        ).withColumn('tailGap', f.greatest(length(f.regexp_extract('dstAlg', r'(\-+)\s*$', 1)),
                                           length(f.regexp_extract('srcAlg', r'(\-+)\s*$', 1)))
        ).withColumn('digitMatch', digit_match('srcAlg', 'dstAlg')
        ).withColumn('nlines', f.size('lineIDs')
        ).withColumn('lineID',
                     f.filter('lineIDs', lambda r: (r['start'] >= col('begin')) &
                              (r['start'] + r['length'] <= col('begin') + length('dstText')))[0]['id']
        ).withColumn('regions',
                     f.filter('regions', lambda r: (r['start'] >= col('begin')) &
                              ((r['start'] + r['length']) <= (col('begin') + length('dstText'))))
        ).withColumn('x', f.array_min('regions.coords.x')
        ).withColumn('y', f.array_min('regions.coords.y')
        ).withColumn('w',
                     f.array_max(f.transform('regions.coords',
                                             lambda r: r['x'] + r['w'])) - col('x')
        ).withColumn('h',
                     f.array_max(f.transform('regions.coords',
                                             lambda r: r['y'] + r['h'])) - col('y')
        ).drop('regions', 'lineIDs'
        ).sort(f.desc('matchRate')
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
