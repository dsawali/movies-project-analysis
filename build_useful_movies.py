import sys
from pyspark.sql import SparkSession, functions, types, Row
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName('Map labels to wikidata').enableHiveSupport().getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

def main(n_directory, in_directory_2, out_directory):
    wiki = spark.read.json(sys.argv[1])
    labels = spark.read.json(sys.argv[2])
    labels.cache()
    wiki = wiki.select(wiki['label'].alias('movie'), wiki['cast_member'], wiki['director'], wiki['genre'], wiki['publication_date'], wiki['made_profit'], wiki['imdb_id'])
    #clean the nulls
    wiki = wiki.filter(wiki['movie'].isNotNull())
    wiki = wiki.filter(wiki['cast_member'].isNotNull())
    wiki = wiki.filter(wiki['director'].isNotNull())
    wiki = wiki.filter(wiki['genre'].isNotNull())
    wiki = wiki.filter(wiki['publication_date'].isNotNull())
    wiki.cache()

    wiki_director = wiki.select(wiki['movie'], wiki['director'])
    wiki_cast = wiki.select(wiki['movie'], wiki['cast_member'])
    wiki_genre = wiki.select(wiki['movie'], wiki['genre'])
    wiki_date = wiki.select(wiki['movie'], wiki['publication_date'])
    wiki_profit = wiki.select(wiki['movie'], wiki['made_profit'])
    wiki_profit = wiki_profit.filter(wiki_profit['made_profit'].isNotNull())
    
    wiki_imdb = wiki.select(wiki['movie'], wiki['imdb_id'])
    wiki_imdb = wiki_imdb.filter(wiki_imdb['imdb_id'].isNotNull())

    # explode values
    wiki_director = wiki_director.withColumn('director', explode(wiki_director.director))
    wiki_cast = wiki_cast.withColumn('cast_member', explode(wiki_cast.cast_member))
    wiki_genre = wiki_genre.withColumn('genre', explode(wiki_genre.genre))

    #join labels
    director_join = wiki_director.join(labels, wiki_director['director'] == labels['wikidata_id'])
    director_join = director_join.select(director_join['movie'], director_join['label'].alias('director')).dropDuplicates()
    
    cast_join = wiki_cast.join(labels, wiki_cast['cast_member'] == labels['wikidata_id'])
    cast_join = cast_join.select(cast_join['movie'], cast_join['label'].alias('cast_member')).dropDuplicates()
    
    genre_join = wiki_genre.join(labels, wiki_genre['genre'] == labels['wikidata_id'])
    genre_join = genre_join.select(genre_join['movie'], genre_join['label'].alias('genre')).dropDuplicates()
    
    # merge everything
    merged = director_join.join(cast_join, on='movie', how='outer')
    merged = merged.join(genre_join, on='movie', how='outer')
    

    merged = merged.groupBy('movie').agg(functions.collect_set('director').alias('director'), functions.collect_set('cast_member').alias('cast_member'), functions.collect_set('genre').alias('genre'))
    merged = merged.join(wiki_date, on='movie', how='outer')
    merged = merged.join(wiki_profit, on='movie', how='outer')
    merged = merged.join(wiki_imdb, on='movie', how='outer')
    
    merged.show(n=20)
   

    merged.coalesce(1).write.json('wikidata_readable', mode='overwrite', compression='gzip')

if __name__=='__main__':
    in_directory = sys.argv[1]
    in_directory_2 = sys.argv[2]
    out_directory = sys.argv[3]
    main(in_directory, in_directory_2, out_directory)
    # main(in_directory, in_directory_2, out_directory)