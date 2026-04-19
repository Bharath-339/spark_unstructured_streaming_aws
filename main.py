from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

from config import configs as configuration
from udf_utils import *


def define_udf():
    return {
        "extract_file_name_udf": udf(extract_file_name, StringType()),
        "extract_position_udf": udf(extract_position, StringType()),
        "extract_classcode_udf": udf(extract_classcode, StringType()),
        "extract_salary_start_udf": udf(
            extract_salary,
            StructType(
                [
                    StructField("salary_start", DoubleType(), True),
                    StructField("salary_end", DoubleType(), True),
                ]
            ),
        ),
        "extract_start_date_udf": udf(extract_start_date, DateType()),
        "extract_end_date_udf": udf(extract_end_date, DateType()),
        "extract_requirements_udf": udf(extract_requirements, StringType()),
        "extract_notes_udf": udf(extract_notes, StringType()),
        "extract_duties_udf": udf(extract_duties, StringType()),
        "extract_selection_udf": udf(extract_selection, StringType()),
        "extract_experience_length_udf": udf(extract_experience_length, StringType()),
        "extract_job_type_udf": udf(extract_job_type, StringType()),
        "extract_education_length_udf": udf(extract_education_length, StringType()),
        "extract_school_type_udf": udf(extract_school_type, StringType()),
        "extract_application_location_udf": udf(
            extract_application_location, StringType()
        ),
    }

def format_text_df(text_df):

    text_df = text_df.withColumn("file_name", regexp_replace(udfs["extract_file_name_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("position", regexp_replace(udfs["extract_position_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("classcode", regexp_replace(udfs["extract_classcode_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("salary_start", udfs["extract_salary_start_udf"]("value").getItem("salary_start"))
    text_df = text_df.withColumn("salary_end", udfs["extract_salary_start_udf"]("value").getItem("salary_end"))
    text_df = text_df.withColumn("start_date", udfs["extract_start_date_udf"]("value"))
    text_df = text_df.withColumn("end_date", udfs["extract_end_date_udf"]("value"))
    text_df = text_df.withColumn("req", regexp_replace(udfs["extract_requirements_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("notes", regexp_replace(udfs["extract_notes_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("duties", regexp_replace(udfs["extract_duties_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("selection", regexp_replace(udfs["extract_selection_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("experience_length",
                                 regexp_replace(udfs["extract_experience_length_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("job_type", regexp_replace(udfs["extract_job_type_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("education_length",
                                 regexp_replace(udfs["extract_education_length_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("school_type", regexp_replace(udfs["extract_school_type_udf"]("value"), "\r", ""))
    text_df = text_df.withColumn("application_location",
                                 regexp_replace(udfs["extract_application_location_udf"]("value"), "\r", ""))
    return text_df.select(
        "file_name",
        "position",
        "classcode",
        "salary_start",
        "salary_end",
        "start_date",
        "end_date",
        "req",
        "notes",
        "duties",
        "selection",
        "experience_length",
        "job_type",
        "education_length",
        "school_type",
        "application_location",
    )

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("spark_unstructured_streaming_aws")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    text_input_dir = "./input/input_text"
    json_input_dir = "./input/input_json"
    csv_input_dir = "./input/input_csv"
    pdf_input_dir = "./input/input_pdf"
    video_input_dir = "./input/input_video"
    image_input_dir = "./input/input_image"

    schema = StructType(
        [
            StructField("file_name", StringType(), True),
            StructField("position", StringType(), True),
            StructField("classcode", StringType(), True),
            StructField("salary_start", DoubleType(), True),
            StructField("salary_end", DoubleType(), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True),
            StructField("req", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("duties", StringType(), True),
            StructField("selection", StringType(), True),
            StructField("experience_length", StringType(), True),
            StructField("job_type", StringType(), True),
            StructField("education_length", StringType(), True),
            StructField("school_type", StringType(), True),
            StructField("application_location", StringType(), True),
        ]
    )
    udfs = define_udf()

    text_df = spark.readStream.format("text").option("wholetext", "true").load(text_input_dir)
    json_df = spark.readStream.format("json").schema(schema).load(json_input_dir)
    csv_df = spark.readStream.format("csv").option("header", "true").schema(schema).load(csv_input_dir)

    # Apply UDFs to the text DataFrame to extract structured fields
    text_df = format_text_df(text_df)

    final_df = text_df.unionByName(json_df).unionByName(csv_df)

    def streamWriter(df, output_dir):
        return (df.writeStream
                    .format("parquet")
                    .trigger("processingTime", "2 minute")
                    .option("checkpointLocation", f'{output_dir}/checkpoint')
                    .outputMode("append")
                    .start(f'{output_dir}/data')
                )

    s3_bucket_name = configuration.get("S3_BUCKET_NAME")
    s3_dir = f's3a://{s3_bucket_name}/unstructured_streaming/'

    query = streamWriter(final_df,s3_dir)
    query.awaitTermination()