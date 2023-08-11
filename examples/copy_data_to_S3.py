
from pyspark.sql import SparkSession
import boto3
import configparser


# load parameters for config

config = configparser.ConfigParser()
config.read_file(open('/Users/melissabee/Documents/GitHub/udacity_aws_glue/aws.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
VPC                    = config.get('AWS','VPC')
ROUTE                  = config.get('AWS','ROUTE')     

# Create a Spark session

spark = SparkSession.builder \
    .appName("Stedi") \
.getOrCreate()


# Read data from a source 
df = spark.read.json("data/customer/customer-keep-1655293787679.json")

# Write the data to an S3 bucket
df.write \
    .mode("overwrite") \
    .json("s3://stedi-aws-project/landing/customer")