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

# Script generated for node Customer Curated
CustomerCurated_node1717042289065 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1717042289065")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1717044110420 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1717044110420")

# Script generated for node Join
Join_node1717042391809 = Join.apply(frame1=CustomerCurated_node1717042289065, frame2=StepTrainerLanding_node1717044110420, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1717042391809")

# Script generated for node Drop Fields
DropFields_node1717042414645 = DropFields.apply(frame=Join_node1717042391809, paths=["`.serialnumber`", "sharewithpublicasofdate", "phone", "lastupdatedate", "email", "sharewithfriendsasofdate", "customername", "registrationdate", "sharewithresearchasofdate", "birthday"], transformation_ctx="DropFields_node1717042414645")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1717042469254 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1717042414645, connection_type="s3", format="json", connection_options={"path": "s3://tsp-lakehouse/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1717042469254")

job.commit()