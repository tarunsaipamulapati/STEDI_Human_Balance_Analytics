import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1717042289065 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1717042289065")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1717044110420 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1717044110420")

# Script generated for node Join
Join_node1717042391809 = Join.apply(frame1=CustomerCurated_node1717042289065, frame2=StepTrainerLanding_node1717044110420, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1717042391809")

# Script generated for node SQL Query
SqlQuery0 = '''
select serialNumber,sensorReadingTime,distanceFromObject  from myDataSource

'''
SQLQuery_node1717048341916 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1717042391809}, transformation_ctx = "SQLQuery_node1717048341916")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1717042469254 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1717048341916, connection_type="s3", format="json", connection_options={"path": "s3://tsp-lakehouse/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1717042469254")

job.commit()