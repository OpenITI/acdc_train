import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, slice, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree
from PIL import Image

ns = {'alto': 'http://www.loc.gov/standards/alto/ns-v4#'}
nspref = f'{{{ns["alto"]}}}'

def genAlto(path, img, width, height, lines):
    from lxml.builder import E

    S = 1

    with Image.open(img) as fimg:
        iw, ih = fimg.size
        S = 1 / round(width / iw)

    lelem = []
    lid = 0
    for line in lines:
        if line.srcText != None:
            content = line.srcText.strip()
            if len(content) > 0:
                w, h = str(line.w * S), str(line.h * S)
                x1, y1 = str(line.x * S), str(line.y * S)
                x2, y2 = str((line.x + line.w) * S), str((line.y + line.h) * S)
                elem = E.TextLine(
                    E.Shape( E.Polygon(POINTS=' '.join([x1, y1, x1, y2, x2, y2, x2, y1]))),
                    E.String(CONTENT=content, HPOS=x1, VPOS=y1, WIDTH=w, HEIGHT=h),
                    HPOS=x1, VPOS=y1, WIDTH=w, HEIGHT=h,
                    BASELINE=' '.join([x1, y2, x2, y2]))
                lelem.append(elem)
    
    doc = E.alto(
        E.Description( E.MeasurementUnit('pixel'),
                       E.sourceImageInformation( E.fileName(img) ) ),
        E.Tags( E.OtherTag(ID='TYPE_1', LABEL='default') ),
        E.Layout(
            E.Page(
                E.PrintSpace(
                    E.TextBlock(*lelem, ID='eSc_dummyblock_'),
                    HPOS='0', VPOS='0', WIDTH=str(width * S), HEIGHT=str(height * S)),
                WIDTH=str(width * S), HEIGHT=str(height * S), ID='page_0')),
        xmlns='http://www.loc.gov/standards/alto/ns-v4#')
    
    # print(etree.tostring(doc, pretty_print=True, encoding='UTF-8', xml_declaration=True))
    tree = etree.ElementTree(doc)

    os.makedirs(os.path.dirname(path), exist_ok=True)
    tree.write(path, encoding='UTF-8', xml_declaration=True, pretty_print=True)
    return len(lines)

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

    base = '/work/proj_cssh/nulab/corpora/chroniclingamerica/data/batches/'

    gen_alto = udf(lambda fout, img, width, height,
                   lines: genAlto(config.outputPath+'/'+fout, base+img, width, height, lines),
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

    lines = elect.groupBy('img', 'width', 'height', 'begin', 'x', 'y', 'w', 'h'
                ).agg((f.max(struct('matchRate', 'srcText'))['srcText']).alias('srcText')
                ).groupBy('img', 'width', 'height'
                ).agg(sort_array(collect_list(struct('begin', 'x', 'y', 'w', 'h',
                                                     'srcText'))).alias('lines')
                ).withColumn('fname', f.concat(f.monotonically_increasing_id().cast('string'),
                                               f.lit('.xml'))
                ).select(gen_alto('fname', 'img', 'width', 'height', 'lines').alias('result')
                ).select(f.sum('result').alias('lines')).collect()

    print('# lines: ', lines)
    
    spark.stop()
