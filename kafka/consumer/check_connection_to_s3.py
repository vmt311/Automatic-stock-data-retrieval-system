import os
import logging
from kafka import KafkaConsumer, KafkaProducer
import boto3
from dotenv import load_dotenv

load_dotenv()

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_kafka_connection(bootstrap_servers: str, topic: str = "test_connection"):
    """Kiểm tra kết nối đến Kafka broker."""
    try:
        # Test Producer (ghi 1 message test)
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        producer.send(topic, value="test_message").get(timeout=10)
        logger.info("✅ Kafka Producer: Kết nối thành công!")

        # Test Consumer (đọc message vừa ghi)
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        for msg in consumer:
            logger.info(f"✅ Kafka Consumer: Nhận được message - {msg.value.decode('utf-8')}")
            break
        else:
            raise Exception("Không nhận được message test!")

    except Exception as e:
        logger.error(f"❌ Lỗi kết nối Kafka: {str(e)}")
        raise

def check_s3_connection(bucket_name: str):
    """Kiểm tra kết nối và quyền truy cập AWS S3."""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        
        # Kiểm tra list buckets (quyền cơ bản)
        s3.list_buckets()
        logger.info("✅ AWS S3: Kết nối thành công!")

        # Kiểm tra quyền ghi (nếu có bucket name)
        if bucket_name:
            test_key = "connection_test.txt"
            s3.put_object(Bucket=bucket_name, Key=test_key, Body="test")
            s3.delete_object(Bucket=bucket_name, Key=test_key)
            logger.info(f"✅ AWS S3: Có quyền ghi/xóa trên bucket '{bucket_name}'")

    except Exception as e:
        logger.error(f"❌ Lỗi kết nối AWS S3: {str(e)}")
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", help="Kafka bootstrap servers (e.g., localhost:9092)")
    parser.add_argument("--s3-bucket", help="AWS S3 bucket name (optional)")
    args = parser.parse_args()

    try:
        if args.kafka:
            check_kafka_connection(args.kafka)
        if args.s3_bucket:
            check_s3_connection(args.s3_bucket)
        logger.info("🎉 Tất cả kết nối hoạt động tốt!")
    except Exception:
        logger.error("🔥 Có lỗi xảy ra!")
