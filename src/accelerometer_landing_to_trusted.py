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

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://quannguyenchauthao-stedi-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingZone_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1679499248306 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lakehouse",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1679499248306",
)

# Script generated for node ApplyJoinMapping
ApplyJoinMapping_node2 = Join.apply(
    frame1=AccelerometerLandingZone_node1,
    frame2=CustomerTrustedZone_node1679499248306,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="ApplyJoinMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1679499304461 = DropFields.apply(
    frame=ApplyJoinMapping_node2,
    paths=[
        "serialnumber",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1679499304461",
)

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node3 = glueContext.getSink(
    path="s3://quannguyenchauthao-stedi-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrustedZone_node3",
)
AccelerometerTrustedZone_node3.setCatalogInfo(
    catalogDatabase="stedi-lakehouse", catalogTableName="accelerometer_trusted"
)
AccelerometerTrustedZone_node3.setFormat("json")
AccelerometerTrustedZone_node3.writeFrame(DropFields_node1679499304461)
job.commit()
