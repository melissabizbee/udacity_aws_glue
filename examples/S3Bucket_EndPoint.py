
from pyspark.sql import SparkSession
import boto3
import configparser


# load parameters for config

config = configparser.ConfigParser()
config.read_file(open('/Users/melissabee/Documents/GitHub/udacity_aws_glue/aws.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
VPC               = config.get('AWS','VPC')
ROUTE            = config.get('AWS','ROUTE')                  


# Create a Spark session

spark = SparkSession.builder \
    .appName("Stedi") \
.getOrCreate()

# Create an S3 and vpc client using Boto3
s3_client = boto3.client('s3', region_name='us-west-2')
ec2_client = boto3.client('ec2', region_name='us-west-2')

# Specify the name of your bucket
bucket_name = 'stedi-aws-project'

# Create the S3 bucket
s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

# Print the confirmation message
print(f"S3 bucket '{bucket_name}' created successfully!")

# Create the S3 Gateway Endpoint
response = ec2_client.create_vpc_endpoint(
    VpcId=VPC,
    ServiceName='com.amazonaws.us-west-2.s3',
    RouteTableIds=[ROUTE]
)

# Print the endpoint details
print(f"endpoint '{response}' created successfully!")