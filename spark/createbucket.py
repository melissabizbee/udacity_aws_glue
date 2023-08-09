
# load parameters for config

import configparser
config = configparser.ConfigParser()
config.read_file(open('/Users/melissabee/Documents/GitHub/udacity_aws_glue/aws.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')


# Create a Spark session

from pyspark.sql import SparkSession
import boto3

spark = SparkSession.builder \
    .appName("Spark AWS") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
.getOrCreate()

# Create an S3 client using Boto3
s3_client = boto3.client('s3')

# Specify the name of your bucket
bucket_name = 'project'

# Create the S3 bucket
s3_client.create_bucket(Bucket=bucket_name)

# Print the confirmation message
print(f"S3 bucket '{bucket_name}' created successfully!")