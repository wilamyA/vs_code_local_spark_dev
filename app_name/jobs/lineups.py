from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app_name.configs import global_variables
import boto3


spark = SparkSession.builder.getOrCreate()

# return client boto S3
def return_client_s3(endpoint, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.client(
                            "s3",
                            endpoint_url=endpoint,
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key
                        )
    return s3
    

# goes through all pages in the lineups folder and returns a list of file names
# Note: Need to scroll through all pages. S3 paging every 1000 objects, so I'm going through all the pages to capture all the objects.    
def get_list_matches(bucket, prefix):
    
    path_lineups = global_variables.LINEUPS
    
    s3 = return_client_s3(endpoint=global_variables.MINIO_ENDPOINT, aws_access_key_id=global_variables.AWS_ACCESS_KEY_ID, aws_secret_access_key=global_variables.AWS_SECRET_ACCESS_KEY)
    paginator = s3.get_paginator('list_objects_v2')
    # get all files from the lineups folder
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    list_matches = []

    for page in pages:
        for obj in page["Contents"]:
        # get the name of files without prefix and without extension
            file_name = str(obj["Key"].replace(global_variables.PREFIX_BUCKET_ORIGIN+path_lineups+"/", "")).replace(".json", "")
            list_matches.append(file_name)
    
    return list_matches



def save_landing_lineups(path_origin_matches, path_landing_lineups):
    # Go through the entire list of games and create the 'match_id' column and play in the landing layer
    # Note: I chose not to use partitionBy because the files are already separated.
    
    list_matches = get_list_matches(bucket=global_variables.BUCKET_ORIGIN, prefix=global_variables.PREFIX_BUCKET_ORIGIN+"lineups")
    
    for match in list_matches:       
        path_match =  path_origin_matches+match+".json"
        df_lineups = spark.read.option("multiline", "true").json(path_match)
        df_lineups_match_id = df_lineups.withColumn("match_id", F.lit(match))
        df_lineups_match_id.write.format("json").save(path_landing_lineups+"match_id="+match)


def save_bronze_lineups(path_origin,path_landing_matches,path_bronze_lineups):
    # Note: The games table will be necessary to get the partition columns that do not exist in lineups
    df_matches = spark.read.json(path_landing_matches)
    df_lineups = spark.read.json(path_origin)
    
    df_lineups_matches = df_lineups.join(df_matches, "match_id").select(df_lineups.lineup,df_lineups.match_id,df_lineups.team_id, df_lineups.team_name, df_matches.competition_id,df_matches.season_id)

    df_lineups_matches.write.partitionBy("competition_id","season_id","team_id").parquet(path_bronze_lineups)


if __name__ == '__main__':
     
    '''print("------------- saving lineups in landingzone -----------------------------------------")
    save_landing_lineups(path_origin_matches="s3://"+global_variables.BUCKET_ORIGIN+"/"+global_variables.PREFIX_BUCKET_ORIGIN+"lineups/", \
                            path_landing_lineups="s3://"+global_variables.BUCKET_LANDING+"lineups/")
    print("------------- matches file saved successfully in landingZone ----------------------------------------")'''
    
    print("------------- saving lineups in bronze layer -----------------------------------------")
    save_bronze_lineups(path_origin="s3://"+global_variables.BUCKET_LANDING+"lineups", path_landing_matches="s3://"+global_variables.BUCKET_LANDING+"matches", path_bronze_lineups="s3://"+global_variables.BUCKET_BRONZE+"lineups")
    print("------------- lineups file saved successfully on the bronze layer ----------------------")
