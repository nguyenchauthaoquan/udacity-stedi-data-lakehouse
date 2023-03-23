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
CustomerTrustedZone_node1679501265208 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lakehouse",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedZone_node1679501265208",
)

# Script generated for node ApplyJoinMapping
ApplyJoinMapping_node2 = Join.apply(
    frame1=AccelerometerLandingZone_node1,
    frame2=CustomerTrustedZone_node1679501265208,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="ApplyJoinMapping_node2",
)

# Script generated for node SQL Query
SqlQuery1531 = """
select * from myDataSource 
where timestamp >= shareWithResearchAsOfDate
"""
SQLQuery_node1679541197827 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1531,
    mapping={"myDataSource": ApplyJoinMapping_node2},
    transformation_ctx="SQLQuery_node1679541197827",
)

# Script generated for node Drop Fields
DropFields_node1679501393679 = DropFields.apply(
    frame=SQLQuery_node1679541197827,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1679501393679",
)

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1679501393679,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://quannguyenchauthao-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCuratedZone_node3",
)

job.commit()
