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

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1679503421946 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi-lakehouse",
        table_name="step_trainer_trusted",
        transformation_ctx="StepTrainerTrustedZone_node1679503421946",
    )
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lakehouse",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrustedZone_node1",
)

# Script generated for node ApplyJoinMapping
ApplyJoinMapping_node2 = Join.apply(
    frame1=AccelerometerTrustedZone_node1,
    frame2=StepTrainerTrustedZone_node1679503421946,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="ApplyJoinMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1679503544811 = DropFields.apply(
    frame=ApplyJoinMapping_node2,
    paths=["user"],
    transformation_ctx="DropFields_node1679503544811",
)

# Script generated for node Step Trainer Curated Zone
StepTrainerCuratedZone_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1679503544811,
    database="stedi-lakehouse",
    table_name="machine_learning_curated",
    transformation_ctx="StepTrainerCuratedZone_node3",
)

job.commit()
