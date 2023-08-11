# Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings 
# from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.


# see ./images/accelerometer_trusted.png for query results in Athena



import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node CustomerTrusted
CustomerTrusted_node1691699664944 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-aws-project/trusted/customer/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1691699664944",
)

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1691699500401 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1691699500401",
)

# Script generated for node JoinCustomer
JoinCustomer_node1691699589416 = Join.apply(
    frame1=AccelerometerLanding_node1691699500401,
    frame2=CustomerTrusted_node1691699664944,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1691699589416",
)

# Script generated for node DropCustomerFields
DropCustomerFields_node1691699809683 = DropFields.apply(
    frame=JoinCustomer_node1691699589416,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropCustomerFields_node1691699809683",
)

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropCustomerFields_node1691699809683,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-aws-project/trusted/accelerometer/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
