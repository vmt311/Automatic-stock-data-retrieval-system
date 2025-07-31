import os
from kafka import KafkaConsumer
import boto3
import json
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

# Cau hinh logging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3KafkaConsumer:
    def __init__(self):
        # Thiet lap tham so moi truong
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
        self.topic = os.getenv('KAFKA_TOPIC', 'stock_prices')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.batch_size = int(os.getenv('BATCH_SIZE', 10))

        print(f"Bucket name from env: {self.bucket_name}")
        logger.info(f"Bucket name: {self.bucket_name}")
        logger.info(f"Connecting to Kafka broker at: {self.kafka_broker}")
        logger.info(f"Subscribing to topic: {self.topic}")

        # 3. Khởi tạo S3 client (AWS credentials từ biến môi trường)
        self.s3 = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )

        # Khoi tao kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers = self.kafka_broker,
            value_deserializer = lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',   # Doc tu dau neu khong co offset
            enable_auto_commit=False,        # Tat tu  dong commit de kiem soat
            group_id='s3-consumer-group'    #Nhom consumer (cho xu ly song song)
        )

        # Kiem tra ket noi Kafka
        try:
            self.consumer.topics()  # Thử lấy danh sách topics
            logger.info(f"Connected to Kafka at {self.kafka_broker}, subscribed to topic {self.topic}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise
        
        # Kiểm tra kết nối S3
        try:
            self.s3.list_buckets()  # Thử list buckets
            logger.info(f"Connected to S3, will upload to bucket {self.bucket_name}")
        except Exception as e:
            logger.error(f"Failed to connect to S3: {str(e)}")
            raise

    def process_messages(self):
        batch=[]
        try:
            # doc message tu kafka
            logger.info("Starting to consume messages...")
            for message in self.consumer:
                logger.debug(f"Received message: {message.value}")
                data = message.value
                # Kiểm tra dữ liệu hợp lệ
                if not isinstance(data, dict):
                    logger.warning(f"Invalid message format: {type(data)}")
                    continue

                batch.append(data)
                
                # upload batch khi du kich thuoc
                if len(batch) >= self.batch_size:
                    logger.info(f"Reached batch size {self.batch_size}, uploading...")
                    self.upload_batch(batch)
                    self.consumer.commit()  # Xác nhận đã xử lý
                    logger.info(f"Committed offset: {message.offset}")
                    batch=[]

        except Exception as e:
            logger.error(f"Error processing messages: {str(e)}", exc_info=True)
            if batch:
                logger.info(f"Uploading remaining {len(batch)} messages before exit")
                self.upload_batch(batch)
    
    def upload_batch(self, batch):
        try:
            # Tao ten file theo thoi gian
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f'stock-data/{timestamp}.json'

            # Chuyển đổi sang JSON và encode thành bytes
            body = json.dumps(batch, ensure_ascii=False, default=str).encode('utf-8')

            # upload len s3 duoi dang json
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=body
            )
            logger.info(f"Upload {len(batch)} record to s3://{self.bucket_name}/{s3_key}")
            
        except Exception as e:
            logger.error(f"Failed to upload batch to S3: {str(e)}")

if __name__ == "__main__":
    consumer = S3KafkaConsumer()
    consumer.process_messages()

