from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import sys

# Thêm đường dẫn để Airflow có thể import producer/consumer từ volume đã mount

sys.path.insert(0, '/opt/airflow')

# Lấy biến môi trường từ Docker container
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock_prices')

# Default arguments cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Định nghĩa DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Pipeline thu thập và xử lý dữ liệu chứng khoán hàng ngày',
    schedule_interval='5 0 * * TUE-SAT',  # Chạy lúc 00h05 từ thứ 3 đến thứ 7 
    catchup=False,  # Không chạy bù cho các lần trước
    max_active_runs=1,  # Chỉ cho phép 1 run tại 1 thời điểm
    tags=['stock', 'kafka', 's3'],
    # ✅ Thêm params để UI hiển thị form nhập ngày khi trigger
    params={
        "run_date": datetime.today().strftime('%Y-%m-%d')  # giá trị mặc định là hôm nay
    }
)

def run_producer(**kwargs):
    """
    Task chạy producer script
    Sử dụng PythonOperator để thực thi producer.py từ volume đã mount
    """
    from kafka.producer.producer import main
    
    # Lấy ngày hiện tại theo định dạng YYYY-MM-DD
    execution_date = kwargs['execution_date']
    # Lấy ngày từ params (UI nhập vào) hoặc từ logical date của Airflow
    if kwargs['params'].get('run_date'):
        run_date = kwargs['params']['run_date']
    else:
        run_date = (kwargs['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    # Ngày cần xử lý là ngày hôm trước
    print(f"🔄 Bắt đầu producer cho ngày {run_date}")
    
    # Gọi hàm main từ producer
    main(run_date)

def run_consumer(**kwargs):
    """
    Task chạy consumer script
    Sử dụng PythonOperator để thực thi consumer.py từ volume đã mount
    """
    from kafka.consumer import consumer # Import từ /app/consumer (đã mount trong Docker)
    
    print("🔄 Bắt đầu consumer...")
    
    # Khởi tạo và chạy consumer
    stock_consumer = consumer.StockDataConsumer(
        topic=KAFKA_TOPIC,
        kafka_broker=KAFKA_BROKER,
        aws_access_key=AWS_ACCESS_KEY,
        aws_secret_key=AWS_SECRET_KEY,
        bucket_name=S3_BUCKET,
        aws_region=AWS_REGION
    )
    stock_consumer.consume_and_upload()

# Định nghĩa các task
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

producer_task = PythonOperator(
    task_id='run_producer',
    python_callable=run_producer,
    provide_context=True,
    dag=dag,
)

consumer_task = PythonOperator(
    task_id='run_consumer',
    python_callable=run_consumer,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)


spark_etl_task = SparkSubmitOperator(
    task_id='run_spark_etl',
    application="/opt/bitnami/spark/jobs/spark_etl.py", #mount tuừ host
    conn_id="spark_default",
    # jars="/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/hadoop-common-3.3.4.jar,/opt/spark/jars/postgresql-42.6.0.jar",
    conf={
        # override
        "spark.driver.extraJavaOptions": "-Dfs.s3a.connection.timeout=60000",
        "spark.executor.extraJavaOptions": "-Dfs.s3a.connection.timeout=60000",
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
        # "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
        # "spark.hadoop.fs.s3a.socket.timeout": "60000",

        
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem", # Cấu hình này là BẮT BUỘC
        "spark.hadoop.fs.s3a.endpoint": f"s3.{AWS_REGION}.amazonaws.com", # Thêm endpoint cho S3
        "spark.hadoop.fs.s3a.path.style.access": "false", # Đặt false cho AWS S3
        # TODO
        "spark.jars.packages": ",".join([
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",  # thay vì chỉ aws-java-sdk-s3
            "org.postgresql:postgresql:42.6.0"              # JDBC driver cho Postgres
        ]),
        
    },
    env_vars={
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_KEY,
        'AWS_REGION': AWS_REGION,
        'S3_BUCKET_NAME': S3_BUCKET
    },
    dag=dag
)

# Thiết lập dependencies
start_task >> producer_task >> consumer_task >> spark_etl_task >> end_task