from pyspark.sql import SparkSession


def save_landing_competition(path_origin, path_landing_competition):
    
    spark = SparkSession.builder.getOrCreate()
    
    # read competition
    df_competition = spark.read.option("multiline","true").json(path_origin)
    
    # write competition in LandingZone
    # Passar partições como parãmetro
    df_competition.write.partitionBy("season_id").format("json").mode("overwrite").save(path_landing_competition)
    
    spark.stop()


def save_bronze_competition(path_origin, path_bronze_competition):
    
    spark = SparkSession.builder.getOrCreate()
    
    df_competition_landing = spark.read.json(path_origin)
    
    df_competition_landing.write.partitionBy("season_id").parquet(path_bronze_competition)
    
    spark.stop()


def save_silver_competition():
    pass


if __name__ == '__main__':
    
    print("------------- saving competitions in landingzone -----------------------------------------")
    save_landing_competition(path_origin='s3://sor/football/data/competitions.json', path_landing_competition='s3://landing/competitions')
    print("------------- competitions file saved successfully in landingZone ----------------------------------------")

    print("------------- saving competitions in bronze layer -----------------------------------------")
    save_bronze_competition(path_origin='s3://landing/competitions', path_bronze_competition='s3://bronze/competitions')
    print("------------- Competition file saved successfully on the bronze layer ----------------------")
    