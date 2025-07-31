from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
import os
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def configure_spark():
    """Cấu hình Spark Session với các thông số tối ưu cho S3"""
    spark = SparkSession.builder \
        .appName("StockDataProcessor") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "1") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
        .getOrCreate()
    
    # Đảm bảo cấu hình AWS được áp dụng
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    sc._jsc.hadoopConfiguration().set("fs.s3a.region", os.getenv("AWS_REGION"))
    
    return spark

def main():
    try:
        logger.info("Starting Spark session...")
        spark = configure_spark()
        logger.info("Spark session initialized successfully")

        # Đọc dữ liệu từ S3
        s3_path = f"s3a://{os.getenv('S3_BUCKET_NAME')}/stock-data/*.json"
        logger.info(f"Reading data from S3 path: {s3_path}")
        
        df = spark.read \
            .option("inferSchema", "true") \
            .option("maxFilesPerTrigger", "100") \
            .json(s3_path)
        logger.info(f"Successfully read {df.count()} records from S3")

        # Xử lý dữ liệu
        processed_df = process_data(df)
        processed_df.printSchema()
        processed_df.show(5)

        # Ghi vào PostgreSQL
        write_to_postgres(processed_df)
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

def process_data(df):
    """Xử lý dữ liệu chính"""
    return df \
        .withColumn("datetime", to_timestamp(col("datetime"))) \
        .withColumn("processed_at", current_timestamp()) \
        .dropDuplicates(["ticker", "datetime"])

def write_to_postgres(df):
    """Ghi dữ liệu vào PostgreSQL"""
    logger.info("Writing data to PostgreSQL")
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/stockdb") \
        .option("dbtable", "stock_prices") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("createTableColumnTypes", """
            ticker VARCHAR(10),
            name VARCHAR(50),
            datetime TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            processed_at TIMESTAMP
        """) \
        .option("batchsize", 5000) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()
    logger.info("Data successfully written to PostgreSQL")

if __name__ == "__main__":
    main()