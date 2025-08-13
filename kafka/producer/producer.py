import os
import json
import yfinance as yf
from datetime import datetime, timedelta
from confluent_kafka import Producer
import pytz
import sys

from dotenv import load_dotenv

load_dotenv()

# Cấu hình Kafka từ biến môi trường
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock_prices')

# Danh sách cổ phiếu quan tâm
TICKERS = [
    {"symbol": "MSFT", "name": "Microsoft"},
    {"symbol": "AAPL", "name": "Apple"},
    {"symbol": "GOOG", "name": "Alphabet"},
    {"symbol": "AMZN", "name": "Amazon"},
    {"symbol": "META", "name": "Meta"},
    {"symbol": "TSLA", "name": "Tesla"},
    {"symbol": "NVDA", "name": "Nvidia"},
    {"symbol": "PYPL", "name": "PayPal"},
    {"symbol": "ADBE", "name": "Adobe"},
    {"symbol": "NFLX", "name": "Netflix"}
]

def delivery_report(err, msg):
    """Callback xác nhận gửi message thành công hay thất bại"""
    if err is not None:
        print(f'Gửi message thất bại: {err}')
    else:
        print(f'Message đã gửi đến [{msg.topic()}] partition [{msg.partition()}]')

def create_kafka_producer():
    """Tạo và cấu hình Kafka Producer"""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'message.timeout.ms': 30000,     # 30s timeout
        'enable.idempotence': True,       # Đảm bảo gửi chính xác một lần
        'acks': 'all',                    # Yêu cầu xác nhận từ tất cả replica
        'retries': 5,                     # Số lần thử lại khi gặp lỗi
        'compression.type': 'snappy',     # Nén dữ liệu để tiết kiệm băng thông
    }
    return Producer(conf)

def is_weekday(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    return date_obj.weekday() < 5  # 0-4 là thứ 2 đến thứ 6

def fetch_stock_data(ticker_info, date_str):
    """
    Lấy dữ liệu chứng khoán từ Yahoo Finance
    :param ticker_info: Thông tin cổ phiếu (symbol và name)
    :param date_str: Ngày cần lấy dữ liệu (YYYY-MM-DD)
    :return: Danh sách các bản ghi dữ liệu
    """
    symbol = ticker_info["symbol"]
    try:
        # Chuyển đổi ngày và tính ngày tiếp theo
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        next_day = date_obj + timedelta(days=1)
        
        # Lấy dữ liệu theo phút
        df = yf.download(
            symbol,
            start=date_obj.strftime("%Y-%m-%d"),
            end=next_day.strftime("%Y-%m-%d"),
            interval="1m",
            progress=False,
            auto_adjust=True
        )
        
        if df.empty:
            print(f"⚠️ Không có dữ liệu cho {symbol} ngày {date_str}")
            return []
        
        # Chuyển đổi dữ liệu thành định dạng JSON
        records = []
        for timestamp, row in df.iterrows():
            # Chuyển đổi múi giờ thành UTC
            utc_timestamp = timestamp.tz_convert(pytz.UTC).isoformat()
            
            records.append({
                "symbol": symbol,
                "name": ticker_info["name"],
                "timestamp": utc_timestamp,
                "open": round(float(row["Open"]), 4),  # Thêm .iloc[0]
                "high": round(float(row["High"]), 4),
                "low": round(float(row["Low"]), 4),
                "close": round(float(row["Close"]), 4),
                "volume": int(row["Volume"]),  # Thêm .iloc[0]
                "process_date": date_str
            })
        
        print(f"✅ Đã lấy {len(records)} bản ghi cho {symbol}")
        return records
        
    except Exception as e:
        print(f"❌ Lỗi khi lấy dữ liệu {symbol}: {str(e)}")
        return []

def produce_stock_data(producer, date_str):
    """
    Gửi dữ liệu chứng khoán đến Kafka
    :param producer: Kafka Producer
    :param date_str: Ngày cần xử lý
    :return: Tổng số message đã gửi
    """
    total_messages = 0
    
    for ticker in TICKERS:
        data_points = fetch_stock_data(ticker, date_str)
        
        for data in data_points:
            # Gửi message đến Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=ticker["symbol"].encode('utf-8'),  # Sử dụng symbol làm key
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            total_messages += 1
            
            # Xử lý sự kiện để kích hoạt gửi message
            producer.poll(0)
    
    # Đảm bảo tất cả message đã được gửi
    producer.flush()
    print(f"🚀 Đã gửi tổng cộng {total_messages} message đến Kafka")
    return total_messages

def main(date_str):
    """
    Hàm chính để chạy producer (airflow sẽ gọi hàm này)
    :param date_str: Ngày cần xử lý (YYYY-MM-DD). Nếu không có, sẽ sử dụng ngày hiện tại
    """
    if date_str is None:
        # Sửa thành cách mới để tránh warning và lấy ngày hiện tại với timezone
        date_str = datetime.now(pytz.UTC).strftime("%Y-%m-%d")
    else:
        # Kiểm tra nếu ngày nhập vào là tương lai
        input_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        today = datetime.now(pytz.UTC).date()
        if input_date > today:
            print(f"⚠️ Cảnh báo: Ngày {date_str} là ngày tương lai, sẽ sử dụng ngày hôm nay thay thế")
            date_str = today.strftime("%Y-%m-%d")
    
    print(f"🔄 Bắt đầu producer cho ngày {date_str}")
    kafka_producer = create_kafka_producer()
    total_messages = produce_stock_data(kafka_producer, date_str)
    print(f"🏁 Hoàn thành producer cho ngày {date_str}")

    if not is_weekday(date_str):
        print(f"⚠️ Ngày {date_str} là cuối tuần, không có dữ liệu giao dịch")
        return 0

if __name__ == "__main__":
    # Cho phép chạy độc lập với tham số dòng lệnh
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_arg)