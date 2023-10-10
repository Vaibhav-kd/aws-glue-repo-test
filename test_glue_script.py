import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

# Create a GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())

# Get the source and target S3 paths from the command line arguments
source_path = "s3://your-source-bucket/your-source-folder/"
target_path = "s3://your-target-bucket/your-target-folder/"

# Create a DynamicFrame from the source data
source_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="your-database-name",
    table_name="your-table-name",
    transformation_ctx="source_dynamic_frame"
)

# Perform a simple transformation (example: selecting specific columns)
transformed_dynamic_frame = source_dynamic_frame.select_fields(["column1", "column2", "column3"])

# Write the transformed data to the target S3 location
glueContext.write_dynamic_frame.from_catalog(
    frame=transformed_dynamic_frame,
    database="your-database-name",
    table_name="your-output-table-name",
    transformation_ctx="target_dynamic_frame"
)

# Commit the job
job.commit()
