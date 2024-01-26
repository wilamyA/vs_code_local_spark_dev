from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

def save_landing_matches(path_origin, path_landing_matches):
    # Read all matches
    df_matches = spark.read.option("multiline", "true").json(path_origin)
    df_matches_added_col_competition_id = df_matches.withColumn("competition_id", F.col("competition.competition_id"))
    df_matches_added_columns_competition_id_season_id = df_matches_added_col_competition_id.withColumn("season_id", F.col("season.season_id"))
    df_matches_added_columns_competition_id_season_id.write.partitionBy("competition_id","season_id").format("json").mode("overwrite").save(path_landing_matches)
    

def save_bronze_matches(path_origin, path_bronze_matches):
    df_matches_landing = spark.read.json(path_origin)
    df_matches_landing.write.partitionBy("competition_id","season_id").mode("overwrite").parquet(path_bronze_matches)
    
   
def save_silver_matches():
    pass    


if __name__ == '__main__':
     
    print("------------- saving matches in landingzone -----------------------------------------")
    save_landing_matches(path_origin='s3://sor/football/data/matches/*', path_landing_matches='s3://landing/matches')
    print("------------- matches file saved successfully in landingZone ----------------------------------------")
    
    print("------------- saving matches in bronze layer -----------------------------------------")
    save_bronze_matches(path_origin='s3://landing/matches', path_bronze_matches='s3://bronze/matches')
    print("------------- matches file saved successfully on the bronze layer ----------------------")
