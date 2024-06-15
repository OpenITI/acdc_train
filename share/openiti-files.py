import argparse, os
import regex
from regex import sub
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, regexp_replace, translate, udf

def cleanOpenITI(text):
    text = sub(r'^#META.*\n*', '', text, flags=regex.M)
    text = sub(r'#+OpenITI#\n*', '', text)
    text = sub(r'[ ]+\n', '\n',
               sub(r'\n[ ]+', '\n',
                   sub(r' [ ]+', ' ',
                       sub(r'\p{P}', ' ',
                           sub(r'PageV\S+|ms\d+|\p{M}', '',
                               text)))))
    return text

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import OpenITI',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<input path>', help='input path')
    parser.add_argument('outputPath', metavar='<output path>', help='output path')

    config = parser.parse_args()
    spark = SparkSession.builder.appName('Import OpenITI').getOrCreate()

    wd = os.getcwd()
    wd = wd if wd.startswith('file:') else 'file:' + wd

    clean_openiti = udf(lambda text: cleanOpenITI(text))

    spark.read.load(config.inputPath, format='binaryFile', recursiveFileLookup='true'
        ).filter(~col('path').endswith('.yml') & ~col('path').endswith('.md')
        ).select(regexp_replace('path', '^' + wd + '/', '').alias('id'),
                 clean_openiti(col('content').cast('string')).alias('text')
        ).write.json(config.outputPath, mode='overwrite')

    spark.stop()
