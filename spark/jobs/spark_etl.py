import argparse
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
import os
import logging
import sys
from datetime import datetime
# from dotenv import load_dotenv
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print("\n===== S3A CONFIG =====")
# In toàn bộ config
for k, v in spark.sparkContext.getConf().getAll():
    print(f"{k} = {v}")

# load_dotenv()

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def configure_spark():
    """Cấu hình Spark Session với các thông số tối ưu"""
    spark = SparkSession.builder \
        .appName("StockDataETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_REGION')}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    
    # Đặt cấu hình AWS
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.region", os.getenv("AWS_REGION"))
    
    return spark

def process_data(df):
    """Xử lý dữ liệu chính"""
    return df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("processed_at", current_timestamp()) \
        .dropDuplicates(["symbol", "timestamp"])

def write_to_postgres(df, table_name="stock_prices"):
    """Ghi dữ liệu vào PostgreSQL"""
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/stockdb") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 10000) \
        .mode("append") \
        .save()

def run_etl_process(date_str=None):
    """Hàm chính để chạy ETL, có thể nhận tham số từ Airflow"""
    try:
        logger.info("Starting ETL process")
        spark = configure_spark()
        
        # Xử lý partition nếu có (cho Airflow)
        s3_base = f"s3a://{os.getenv('S3_BUCKET_NAME')}/stock-data/"
        if date_str:
            date_obj = datetime.strftime(date_str, "%Y-%m-%d")
            s3_path = (
                f"{s3_base}"
                f"year={date_obj.year}/"
                f"month={date_obj.month:02d}/"
                f"day={date_obj.day:02d}/*.json"
            )
        else:
            # Đọc toàn bộ dữ liệu
            s3_path = f"{s3_base}*/*/*/*.json"  # Đọc đệ quy qua các thư mục con
        
        logger.info(f"Checking data at: {s3_path}")
        
        # THÊM PHẦN KIỂM TRA DỮ LIỆU TRƯỚC KHI ĐỌC
        # hadoop = spark.sparkContext._jvm.org.apache.hadoop
        # fs = hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
        # path = hadoop.fs.Path(s3_path)
        jvm = spark._jvm
        uri = jvm.java.net.URI(s3_base)  # hoặc s3_path up to bucket
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark.sparkContext._jsc.hadoopConfiguration())
        path = jvm.org.apache.hadoop.fs.Path(s3_path)
        # Kiểm tra xem có file JSON nào tồn tại không
        if not fs.globStatus(path):
            logger.warning(f"No data found at {s3_path}. Skipping ETL process.")
            return True  # Vẫn trả về thành công
        
        # Đọc dữ liệu từ json vào dataframe
        logger.info(f"Reading data from: {s3_path}")
        df = spark.read.json(s3_path)
        
        # Kiểm tra DataFrame có dữ liệu không
        if df.count() == 0:
            logger.warning("No data available after reading. Skipping processing.")
            return True


        processed_df = process_data(df)
        logger.info(f"Processed {processed_df.count()} records")
        
        write_to_postgres(processed_df)
        logger.info("ETL process completed successfully")
        
        return True
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Processing date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    run_etl_process(args.date)  # Truyền date trực tiếp