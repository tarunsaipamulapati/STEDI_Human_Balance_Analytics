import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1716880051421 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrustedZone_node1716880051421")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716883334543 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1716883334543")

# Script generated for node Join
Join_node1716879955851 = Join.apply(frame1=CustomerTrustedZone_node1716880051421, frame2=AccelerometerTrusted_node1716883334543, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1716879955851")

# Script generated for node Drop Fields
DropFields_node1716881036816 = DropFields.apply(frame=Join_node1716879955851, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="DropFields_node1716881036816")

# Script generated for node Drop Duplicates
DropDuplicates_node1716962005968 =  DynamicFrame.fromDF(DropFields_node1716881036816.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716962005968")

# Script generated for node Customer Curated
CustomerCurated_node1716879965473 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1716962005968, connection_type="s3", format="json", connection_options={"path": "s3://tsp-lakehouse/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1716879965473")

job.commit()