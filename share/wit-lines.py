import argparse, math, re
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, length, lit, struct, translate, udf
import pyspark.sql.functions as f

def witLines(lines, pages, min_line=0):
    res = []
    p = 0
    i = 0
    for line in lines:
        if line.wits != None and len(line.wits) > 0:
            wit = line.wits[0]
            dstAlg = wit.alg2
            dstLength = len(dstAlg.replace('-', '').strip())
            if dstLength >= min_line:
                tlen = len(line.text)
                end = line.begin + len(line.text)
                while p < len(pages):
                    while i < len(pages[p].regions) and pages[p].regions[i].start < line.begin:
                        i += 1
                    if i >= len(pages[p].regions):
                        p += 1
                        i = 0
                    else:
                        break
                if p >= len(pages):
                    break
                x1, y1, x2, y2 = math.inf, math.inf, -math.inf, -math.inf
                while (i < len(pages[p].regions) and
                       (pages[p].regions[i].start + pages[p].regions[i].length) <= end):
                    cur = pages[p].regions[i].coords
                    x1 = min(x1, cur.x)
                    y1 = min(y1, cur.y)
                    x2 = max(x2, cur.x + cur.w)
                    y2 = max(y2, cur.y + cur.h)
                    i += 1
                res.append((line.begin, line.text, wit.id, wit.matches,
                            wit.alg.replace('\n', ' '), dstAlg, dstLength,
                            pages[p].id, pages[p].width, pages[p].height,
                            x1, y1, x2 - x1, y2 - y1))
    return res

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
    parser.add_argument('--fields', type=str, nargs='+', default=[],
                        help='List of fields to include')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Witness lines').getOrCreate()

    max_gap = udf(lambda s: maxGap(s), 'int')
    fix_hyphen = udf(lambda src, dst: fixHyphen(src, dst))
    fix_case = udf(lambda src, dst: fixCase(src, dst) if config.fix_case else src)
    digit_match = udf(lambda src, dst: digitMatch(src, dst), 'double')
    sstrip = udf(lambda s: s.strip())

    wit_lines = udf(lambda lines, pages: witLines(lines, pages, config.min_line),
                    'array<struct<begin: int, dstText: string, src: string, matches: int, srcAlg: string, dstAlg: string, dstLength: int, img: string, width: int, height: int, x: int, y: int, w: int, h: int>>')

    raw = spark.read.load(config.inputPath)
    
    raw.filter(col('pages').isNotNull() & (f.size('pages') == 1) & col('pages')[0]['regions'].isNotNull()
        ).select('id', *config.fields, f.size('lines').alias('nlines'),
                 explode(wit_lines('lines', 'pages')).alias('line')
        ).select('id', *config.fields, 'nlines', col('line.*')
        ).withColumn('length', length(sstrip('dstText'))
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
        ).sort(f.desc('matchRate')
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
