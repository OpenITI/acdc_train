import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree

ns = {'alto': 'http://www.loc.gov/standards/alto/ns-v4#'}
nspref = f'{{{ns["alto"]}}}'

def patchAlto(fname, path, recs):
    patch = {}
    for r in recs:
        patch[r.lineID] = r.srcText

    tree = etree.parse(fname)

    imfile = tree.find('//alto:sourceImageInformation/alto:fileName' ,namespaces=ns)
    oldpath = imfile.text
    if not '/' in oldpath:
        oldpath = os.path.dirname(fname) + '/' + oldpath
    imfile.text = os.path.abspath(oldpath)

    counter = 0
    for line in tree.findall('//alto:TextLine', namespaces=ns):
        counter += 1
        for c in line:
            t = c.tag
            if nspref + 'String' == t or nspref + 'SP' == t:
                line.remove(c)
        id = line.get('ID')
        if id in patch:
            s = etree.SubElement(line, nspref + 'String', CONTENT=patch[id].strip())
            attr = s.attrib
            if line.get('HPOS') != None:
                attr['HPOS'] = line.get('HPOS')
            if line.get('VPOS') != None:
                attr['VPOS'] = line.get('VPOS')
            if line.get('WIDTH') != None:
                attr['WIDTH'] = line.get('WIDTH')
            if line.get('HEIGHT') != None:
                attr['HEIGHT'] = line.get('HEIGHT')

    os.makedirs(os.path.dirname(path), exist_ok=True)
    tree.write(path, encoding='UTF-8', xml_declaration=True)
    return len(patch)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Patch Alto',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', '--filter', type=str,
                        default='matchRate > 0.5 AND leadGap = 0 AND tailGap = 0 AND maxGap < 4 AND length = dstLength',
                        help='Filter training lines')
    parser.add_argument('-n', '--lines', type=int,
                        default=None,
                        help='Maximum number of lines per book')
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Patch Alto').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    patch_alto = udf(lambda fin, fout, lines: patchAlto(fin, config.outputPath+'/'+fin, lines),
                     'int').asNondeterministic()

    raw = spark.read.json(config.inputPath).filter(config.filter)

    if config.lines:
        sample = raw.withColumn('book', f.regexp_replace('id', '/[^/]+$', '')
                    ).groupBy('book'
                    ).agg(slice(sort_array(collect_list(struct('matchRate', 'id', 'lineID')),
                                           False),
                                1, config.lines).alias('lines')
                    ).select(explode('lines').alias('line')
                    ).select(col('line.id'), col('line.lineID'))
        elect = raw.join(sample, ['id', 'lineID'], 'left_semi')
    else:
        elect = raw

    lines = elect.groupBy('img', 'lineID'
                ).agg(f.max(struct('matchRate', 'id', 'srcText')).alias('info')
                ).select('img', 'lineID', 'info.*'
                ).groupBy('id'
                ).agg(collect_list(struct('lineID', 'srcText')).alias('lines')
                ).withColumn('fname', f.concat(f.monotonically_increasing_id().cast('string'),
                                               f.lit('.xml'))
                ).select(patch_alto('id', 'fname', 'lines').alias('result')
                ).select(f.sum('result').alias('lines')).collect()

    print('# lines: ', lines)
    
    spark.stop()
