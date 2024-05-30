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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1717050098844 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1717050098844")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1717050100573 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/step_trainer/trusted/"]}, transformation_ctx="StepTrainerTrusted_node1717050100573")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct * from st join atr on st.sensorreadingtime=atr.timestamp;
'''
SQLQuery_node1717050274351 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"atr":AccelerometerTrusted_node1717050098844, "st":StepTrainerTrusted_node1717050100573}, transformation_ctx = "SQLQuery_node1717050274351")

# Script generated for node Amazon S3
AmazonS3_node1717050353895 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1717050274351, connection_type="s3", format="json", connection_options={"path": "s3://tsp-lakehouse/step_trainer/machine_learning_curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1717050353895")

job.commit()