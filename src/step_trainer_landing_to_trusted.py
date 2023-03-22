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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1679502951747 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lakehouse",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone_node1679502951747",
)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://quannguyenchauthao-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingZone_node1",
)

# Script generated for node ApplyJoinMapping
ApplyJoinMapping_node2 = Join.apply(
    frame1=StepTrainerLandingZone_node1,
    frame2=CustomerCuratedZone_node1679502951747,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="ApplyJoinMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1679503035403 = DropFields.apply(
    frame=ApplyJoinMapping_node2,
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
    transformation_ctx="DropFields_node1679503035403",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1679503035403,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://quannguyenchauthao-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrustedZone_node3",
)

job.commit()
