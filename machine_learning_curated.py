import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1717044994404 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1717044994404")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1717045039660 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://tsp-lakehouse/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1717045039660")

# Script generated for node Join
Join_node1717045258834 = Join.apply(frame1=AccelerometerTrusted_node1717044994404, frame2=StepTrainerTrusted_node1717045039660, keys1=["timestamp"], keys2=["sensorReadingTime"], transformation_ctx="Join_node1717045258834")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1717045290330 = glueContext.write_dynamic_frame.from_options(frame=Join_node1717045258834, connection_type="s3", format="json", connection_options={"path": "s3://tsp-lakehouse/step_trainer/machine_learning_curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1717045290330")

job.commit()