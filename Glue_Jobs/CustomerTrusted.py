
# Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed 
# to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.

# see ./images/customer_trusted.png for query results in Athena


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

# Script generated for node CustomerLanding
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-aws-project/landing/customer/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1691677353487 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="PrivacyFilter_node1691677353487",
)

# Script generated for node CustomerTrusted
CustomerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1691677353487,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-aws-project/trusted/customer/",
        "compression": "gzip",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node3",
)

job.commit()
