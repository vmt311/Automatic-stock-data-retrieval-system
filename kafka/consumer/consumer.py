import os
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

class StockDataConsumer:
    def __init__(self, topic, kafka_broker, aws_access_key, aws_secret_key, bucket_name, aws_region):
        self.topic = topic
        self.kafka_broker = kafka_broker
        self.bucket_name = bucket_name
        
        # Cấu hình Kafka Consumer
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_broker,
            'group.id': 's3-stock-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'isolation.level': 'read_committed',
            'max.poll.interval.ms': 300000,
            # Thêm cấu hình để debug
            'debug': 'all',
            'log_level': 7
        })
        
        # Cấu hình S3 Client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        self.processed_count = 0
        self.current_date = None

    def upload_to_s3(self, data_batch):
        """Upload batch data lên S3 với cấu trúc thư mục theo ngày"""
        
        # Kiẻm tra None và empty batch
        if not data_batch or not isinstance(data_batch, list) or len(data_batch) == 0:
            print ("⚠️ Batch rỗng hoặc None, không upload")
            return False
        
        # Kiểm tra phần tử đầu tiên
        first_item = data_batch[0]
        print(first_item)
        if not isinstance(first_item, dict):
            print(f"⚠️ Kiểu dữ liệu không hợp lệ: {type(first_item)}")
            return False

        # Lấy ngày từ message đầu tiên trong batch
        process_date = data_batch[0].get('process_date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
        # print(process_date)
        try:
            # Chuẩn bị dữ liệu upload, chuyenr đổi dữ liệu sang JSON string
            # Chuyển đổi dữ liệu sang JSON string
            try:
                # json_str = json.dumps(data_batch, indent=2)  # Sửa thành dumps() thay vì dump()
                json_lines = "\n".join(json.dumps(r, default=str) for r in data_batch)
                # print(json_str)
                # Encode thành bytes trước khi upload
                json_bytes = json_lines.encode('utf-8')
                # print(json_bytes)
            except TypeError as e:
                print(f"❌ Lỗi serialization JSON: {str(e)}")
                return False
            
            # Tạo s3 key
            year, month, day = process_date.split('-')        
            s3_key = f"stock-data/year={year}/month={month}/day={day}/stock_{process_date}_{int(time.time())}.json"
            # print(s3_key)

            # Upload lên s3
            try:
                response = self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=json_bytes,
                    ContentType='application/json'
                )
                
                # Kiểm tra phản hồi từ S3
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    print(f"📤 Đã upload {len(data_batch)} records lên s3://{self.bucket_name}/{s3_key}")
                    return True
                else:
                    print(f"❌ Upload thất bại với mã trạng thái: {response['ResponseMetadata']['HTTPStatusCode']}")
                    return False
            except ClientError as e:
                print(f"❌ Lỗi S3: {e.response['Error']['Message']}")
                return False
            except Exception as e:
                print(f"❌ Lỗi không xác định khi upload: {str(e)}")
                return False
            
        except Exception as e:
            print(f"❌ Lỗi khi upload lên S3: {str(e)}")
            return False

    def process_batch(self, batch):
        """Xử lý một batch message và upload lên S3"""
        # print("Received data:", batch)
        if not batch or not isinstance(batch, list):
            print(f"⚠️ Batch không hợp lệ: {type(batch)}")
            return False
        
        # Lọc bỏ phần tử None
        filtered_batch = [item for item in batch if item is not None]

        if len(filtered_batch) != len(batch):
            print(f"⚠️ Đã lọc bỏ {len(batch) - len(filtered_batch)} None values")

        if not filtered_batch:
            print("⚠️ Batch trống sau khi lọc")
            return False     

        print(f"🔄 Đang xử lý batch {len(filtered_batch)} records...")

        if self.upload_to_s3(filtered_batch):
            self.consumer.commit()
            self.processed_count += len(filtered_batch)
            return True
        return False

    def consume_and_upload(self, max_messages=1000, timeout_min=5):
        """
        Main function để Airflow gọi
        :param max_messages: Số message tối đa trước khi upload
        :param timeout_min: Thời gian tối đa (phút) trước khi dừng
        """
        print(f"👂 Bắt đầu consumer cho topic {self.topic}...")
        try:
            self.consumer.subscribe([self.topic])
        except Exception as e:
            print(f"❌ Lỗi khi subscribe topic: {str(e)}")
            raise
        
        batch = []
        start_time = time.time()
        timeout_sec = timeout_min * 60
        
        try:
            while True:
                # Poll message với timeout 1s
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    # Nếu hết message hoặc timeout
                    if (time.time() - start_time) > timeout_sec:
                        print(f"⏰ Đã đạt timeout {timeout_min} phút")
                        break
                    continue
                    
                if msg.error():
                    error = msg.error()
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("ℹ️ Đã đến cuối partition, tiếp tục...")
                        continue
                    print(f"❌ Lỗi Kafka: {error.str()} (code: {error.code()})")
                    if error.fatal():
                        # Lỗi nghiêm trọng, dừng consumer
                        break
                    continue
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    if data is None:
                        print("⚠️ Dữ liệu JSON là None, bỏ qua")
                        continue
                    batch.append(data)
                    
                    # Upload khi đủ batch hoặc gần timeout
                    if len(batch) >= max_messages or (time.time() - start_time) > (timeout_sec - 30):
                        self.process_batch(batch)
                        batch = []
                        
                except json.JSONDecodeError:
                    print(f"❌ Lỗi giải mã JSON: {msg.value()}")
                except UnicodeDecodeError as e:
                    print(f"❌ Lỗi decode UTF-8: {msg.value()} | Lỗi: {str(e)}")
                except Exception as e:
                    print(f"❌ Lỗi không xác định khi xử lý message: {str(e)}")    
                
                # Thoát nếu đã xử lý đủ message (cho Airflow short-running task)
                if self.processed_count >= (max_messages * 3):  # ~3 batches
                    print(f"✅ Đã xử lý đủ {self.processed_count} messages")
                    break
                    
        except KeyboardInterrupt:
            print("⚠️ Nhận tín hiệu dừng...")
        finally:
            # Xử lý batch cuối cùng trước khi đóng
            if batch:
                self.process_batch(batch)
            
            self.consumer.close()
            print(f"🏁 Dừng consumer. Tổng cộng đã xử lý {self.processed_count} messages")

def main():
    """Hàm main để Airflow gọi"""
    consumer = StockDataConsumer(
        topic=os.getenv('KAFKA_TOPIC', 'stock_prices'),
        kafka_broker=os.getenv('KAFKA_BROKER', 'kafka:9092'),
        # kafka_broker='localhost:29092',
        aws_access_key=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        bucket_name=os.getenv('S3_BUCKET_NAME'),
        aws_region=os.getenv('AWS_REGION', 'ap-southeast-2')
    )
    consumer.consume_and_upload(max_messages=500, timeout_min=10)

def test():
    consumer = StockDataConsumer(
        topic=os.getenv('KAFKA_TOPIC', 'stock_prices'),
        kafka_broker=os.getenv('KAFKA_BROKER', 'kafka:9092'),
        # kafka_broker='localhost:29092',
        aws_access_key=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        bucket_name=os.getenv('S3_BUCKET_NAME'),
        aws_region=os.getenv('AWS_REGION', 'ap-southeast-2')
    )
    test_data = [{"symbol": "TEST", "price": 100.0, "process_date": "2023-01-01"}]
    consumer.upload_to_s3(test_data)

if __name__ == "__main__":
    main()
    # test()