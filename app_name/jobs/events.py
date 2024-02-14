from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app_name.configs import global_variables
from lineups import return_client_s3
import boto3


spark = SparkSession.builder.getOrCreate()


def get_list_matches(bucket, prefix):
    
    path_events = global_variables.EVENTS
    
    s3 = return_client_s3(endpoint=global_variables.MINIO_ENDPOINT, aws_access_key_id=global_variables.AWS_ACCESS_KEY_ID, aws_secret_access_key=global_variables.AWS_SECRET_ACCESS_KEY)
    paginator = s3.get_paginator('list_objects_v2')
    # get all files from the lineups folder
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    list_matches = []

    for page in pages:
        for obj in page["Contents"]:
        # get the name of files without prefix and without extension
            file_name = str(obj["Key"].replace(global_variables.PREFIX_BUCKET_ORIGIN+path_events+"/", "")).replace(".json", "")
            list_matches.append(file_name)
    
    return list_matches


def save_landing_events(path_origin_matches, path_landing_events):
    # Go through the entire list of games and create the 'match_id' column and play in the landing layer
    # Note: I chose not to use partitionBy because the files are already separated.
    
    list_matches = get_list_matches(bucket=global_variables.BUCKET_ORIGIN, prefix=global_variables.PREFIX_BUCKET_ORIGIN+"events")
    
    print(path_origin_matches)
    
    for match in list_matches:       
        path_match =  path_origin_matches+match+".json"
        print(path_match)
        df_events = spark.read.option("multiline", "true").json(path_match)
        df_events_match_id = df_events.withColumn("match_id", F.lit(match))
        df_events_match_id.write.format("json").save(path_landing_events+"match_id="+match)


def save_bronze_events(path_origin,path_landing_matches,path_bronze_events):
    # Note: The games table will be necessary to get the partition columns that do not exist in events
    df_matches = spark.read.json(path_landing_matches)
    df_events = spark.read.json(path_origin)
    
    df_events_matches = df_events.join(df_matches, "match_id").select(df_events["*"], df_matches["competition_id"],df_matches["season_id"])

    df_events_matches.write.partitionBy("competition_id","season_id","match_id").parquet(path_bronze_events)


if __name__ == '__main__':
     
    '''print("------------- saving events in landingzone -----------------------------------------")
    save_landing_events(path_origin_matches="s3://"+global_variables.BUCKET_ORIGIN+"/"+global_variables.PREFIX_BUCKET_ORIGIN+"events/", \
                            path_landing_events="s3://"+global_variables.BUCKET_LANDING+"events/")
    print("------------- matches file saved successfully in landingZone ----------------------------------------")
    '''
    print("------------- saving events in bronze layer -----------------------------------------")
    save_bronze_events(path_origin="s3://"+global_variables.BUCKET_LANDING+"events", path_landing_matches="s3://"+global_variables.BUCKET_LANDING+"matches", path_bronze_events="s3://"+global_variables.BUCKET_BRONZE+"events")
    print("------------- events file saved successfully on the bronze layer ----------------------")
