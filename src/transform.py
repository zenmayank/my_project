!pip install pyspark findspark
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, collect_list, concat_ws,col, floor, when, concat, array, expr, regexp_replace
from functools import reduce

spark = SparkSession.builder.appName("mj").getOrCreate()


## reading csv
albums=spark.read.format("csv").option("header",True).option("inferschema",True).load('/content/spotify-albums_data_2023.csv')
albums.show()
track=spark.read.csv('/content/spotify_tracks_data_2023.csv',header=True,inferSchema=True)

## droping dulicates
dedup_albums = albums.dropDuplicates()
dropped_null = dedup_albums.dropna(subset=['track_name','track_id','artist_id'])

dedup_tracks = track.dropDuplicates()
drop_null = dedup_tracks.dropna(subset=['id'])

## merging all the featured artists columns in featured_artists column

featured_artist = dropped_null.withColumn("featured_artists_array", array([f"artist_{i}" for i in range(1, 12)])) \
       .withColumn("featured_artists", expr("concat_ws(', ', filter(featured_artists_array, x -> x is not null))")) \
       .drop(*[f"artist_{i}" for i in range(1, 12)], "featured_artists_array")\

## renaming artist_0 column
featured_artist = featured_artist.withColumnRenamed("artist_0","main_artist")

## droping extra cols
clean_album=featured_artist.drop('artists','duration_sec')

# cleaning tracks as required
clean_track=track.filter((col("track_popularity") > 50) & (col("explicit")=='false'))

# adding radio_mix column and sorting popularity high to low
radio_mix=clean_album.join(clean_track,clean_album.track_id==clean_track.id,"inner").withColumn("radio_mix", lit("true"))\
.orderBy(col("track_popularity").desc())

# changing column positions, selecting columns
selected_cols = radio_mix.select("track_name","track_id","track_number","album_name",
                                 "album_id","album_type","duration_ms","total_tracks","release_date","label",
                                 "track_popularity","album_popularity","artist_id","main_artist","featured_artists",
                                 "explicit","radio_mix")

#writing to bigquery
selected_cols.write.format("bigquery") \
    .option("table", "project:dataset.table") \
    .option("temporaryGcsBucket", "temp-bucket-name") \
    .mode("overwrite") \
    .save()