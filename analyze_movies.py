import sys
import numpy as np 
import pandas as pd
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.mlib.stat import Statistics
from scipy import stats


spark = SparkSession.builder.appName('Analyze Wikidata Data').enableHiveSupport().getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

def npArrayNormal(df, str):
    arr = np.array(df.select(df[str]))
    result = stats.normaltest(arr)
    return result

def main(in_directory, out_directory):
    wiki = spark.read.json(in_directory).dropDuplicates()
    tomatoes = spark.read.json('rotten-tomatoes.json.gz')
    result = npArrayNormal(tomatoes, 'audience_average')
    print('RESSSUUUUUUUUUUUUULT: ', result)
    # normalize rotten tomatoes ratings
    # tomatoes = tomatoes.withColumn('audience_average', functions.round(tomatoes['audience_average']/5, 2))
    # tomatoes = tomatoes.withColumn('audience_percent', functions.round(tomatoes['audience_percent']/100, 2))
    # tomatoes = tomatoes.withColumn('critic_average', functions.round(tomatoes['critic_average']/10, 2))
    
    # tomatoes.show(n=20)

    # merged.coalesce(5).write.json('wikidata_readable', mode='overwrite', compression='gzip')

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)