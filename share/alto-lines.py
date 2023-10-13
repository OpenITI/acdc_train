import argparse, os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, collect_list, explode, sort_array, struct, udf
import pyspark.sql.functions as f
from io import StringIO, BytesIO
from lxml import etree

ns = {'alto': 'http://www.loc.gov/standards/alto/ns-v4#'}

def textLines(s):
    text = ''
    lines = []
    pages = []
    if s != None and s != '':
        tree = etree.parse(BytesIO(s))
        img = tree.findtext('//alto:sourceImageInformation/alto:fileName', namespaces=ns).strip()
        seq = 0
        for p in tree.findall('//alto:Page', namespaces=ns):
            regions = []
            for line in p.findall('.//alto:TextLine', namespaces=ns):
                ## This should be generalized to support lines w/ and w/o CONTENT attribute.
                start = len(text)
                for w in line.findall('.//alto:String', namespaces=ns):
                    if text != '' and not text[len(text)-1].isspace():
                        text += ' '
                    sstart = len(text)
                    text += w.get('CONTENT')
                    if w.get('HPOS') != None:
                        regions.append((sstart, len(text) - sstart,
                                        (int(w.get('HPOS')), int(w.get('VPOS')),
                                         int(w.get('WIDTH')), int(w.get('HEIGHT')),
                                         int(w.get('HEIGHT')))))
                text += '\n'
                lines.append((start, len(text) - start, line.get('ID')))
            pages.append((img,
                          int(p.get('PHYSICAL_IMG_NR', 0)),
                          int(p.get('WIDTH', 0)), int(p.get('HEIGHT', 0)),
                          regions))
        
    return (text, lines, pages)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Alto lines',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()

    wd = os.getcwd()
    wd = wd if wd.startswith('file:') else 'file:' + wd

    spark = SparkSession.builder.appName('Alto lines').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    text_lines = udf(lambda s: textLines(s),
                     'struct<text: string, lineIDs: array<struct<start: int, length: int, id: string>>, pages: array<struct<id: string, seq: int, width: int, height: int, regions: array<struct<start: int, length: int, coords: struct<x: int, y: int, w: int, h: int, b: int>>>>>>').asNondeterministic()

    spark.read.load(config.inputPath, format='binaryFile', pathGlobFilter='*.xml',
                    recursiveFileLookup='true'
        ).withColumn('info', text_lines('content')
        ).select(f.regexp_replace('path', '^' + wd + '/', '').alias('id'), 'info.*'
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
