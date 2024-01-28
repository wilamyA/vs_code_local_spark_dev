from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app_name.configs import global_variables
import boto3


spark = SparkSession.builder.getOrCreate()

# return client boto S3
def return_client_s3():
    s3 = boto3.client(
                            "s3",
                            endpoint_url=global_variables.MINIO_ENDPOINT,
                            aws_access_key_id=global_variables.AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=global_variables.AWS_SECRET_ACCESS_KEY
                        )
    return s3
    

# goes through all pages in the lineups folder and returns a list of file names
# Note: Need to scroll through all pages. S3 paging every 1000 objects, so I'm going through all the pages to capture all the objects.    
def get_list_matches():
    
    path_lineups = global_variables.LINEUPS
    
    s3 = return_client_s3()
    paginator = s3.get_paginator('list_objects_v2')
    # get all files from the lineups folder
    pages = paginator.paginate(Bucket=global_variables.BUCKET_ORIGIN, Prefix=global_variables.PREFIX_BUCKET_ORIGIN+path_lineups)

    list_matches = []

    for page in pages:
        for obj in page["Contents"]:
        # get the name of files without prefix and without extension
            file_name = str(obj["Key"].replace(global_variables.PREFIX_BUCKET_ORIGIN+path_lineups+"/", "")).replace(".json", "")
            list_matches.append(file_name)
    
    return list_matches


def save_landing_lineups(path_origin, path_landing_lineups):
    # Go through the entire list of games and create the 'match_id' column and play in the landing layer
    # Note: I chose not to use partitionBy because the files are already separated.
    
    list_matches = get_list_matches()
    
    for match in list_matches:        
        path_lineups =  path_origin+match+".json"
        df_lineups = spark.read.option("multiline", "true").json(path_lineups)
        df_lineups_match_id = df_lineups.withColumn("match_id", F.lit(match))
        df_lineups_match_id.write.format("json").save(path_landing_lineups)