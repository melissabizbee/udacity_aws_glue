# Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted 
# that contains the Step Trainer Records data for customers who have accelerometer data 
# and have agreed to share their data for research (customers_curated).


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

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-aws-project/landing/step-trainer/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node CustomerCurated
CustomerCurated_node1691700922082 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1691700922082",
)

# Script generated for node JoinCustomerCurated
JoinCustomerCurated_node1691700942264 = Join.apply(
    frame1=CustomerCurated_node1691700922082,
    frame2=StepTrainerLanding_node1,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinCustomerCurated_node1691700942264",
)

# Script generated for node DropCustomerFields
DropCustomerFields_node1691701085279 = DropFields.apply(
    frame=JoinCustomerCurated_node1691700942264,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropCustomerFields_node1691701085279",
)

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropCustomerFields_node1691701085279,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-aws-project/trusted/steptrainer/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node3",
)

job.commit()
