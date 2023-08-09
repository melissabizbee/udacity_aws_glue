import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sticky-aws-bucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node CustomerPPI Filter
CustomerPPIFilter_node1690888390976 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (row["shareWithResearchAsOfDate"] == 0),
    transformation_ctx="CustomerPPIFilter_node1690888390976",
)

# Script generated for node Trusted for Research
TrustedforResearch_node1690888532052 = glueContext.write_dynamic_frame.from_options(
    frame=CustomerPPIFilter_node1690888390976,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sticky-aws-bucket/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedforResearch_node1690888532052",
)

job.commit()
