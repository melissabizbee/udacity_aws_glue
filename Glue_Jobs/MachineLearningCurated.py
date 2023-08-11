# Create an aggregated table that has each of the Step Trainer Readings, 
# and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data,
# and make a glue table called machine_learning_curated.


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

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-aws-project/landing/step-trainer/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node AccelratorTrusted
AccelratorTrusted_node1691701421494 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelratorTrusted_node1691701421494",
)

# Script generated for node JoinAccData
JoinAccData_node1691701513990 = Join.apply(
    frame1=AccelratorTrusted_node1691701421494,
    frame2=StepTrainerTrusted_node1,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinAccData_node1691701513990",
)

# Script generated for node MachineLearningCreated
MachineLearningCreated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=JoinAccData_node1691701513990,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-aws-project/curated/machinelearning/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCreated_node3",
)

job.commit()
