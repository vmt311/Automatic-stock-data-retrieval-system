from kafka import KafkaProducer
import yfinance as yf
import json
import time
import warnings
# Sử dụng ThreadPoolExecutor để tải song song các ticker
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ẩn các cảnh báo FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer= lambda v: json.dumps(v).encode('utf-8')
)

# Danh sách ticker với thông tin dự phòng
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

# Ham lay gia co phieu, tra ve dang json
def fetch_latest(ticker_info):
    """Lấy dữ liệu chứng khoán với xử lý lỗi chi tiết"""
    ticker = ticker_info["symbol"]
    try:
        # Thêm timeout và retry
        df = yf.download(
            ticker,
            period="1d",
            interval="1m",
            auto_adjust=False,
            progress=False,
            timeout=10
        )
        
        if df.empty:
            print(f"No data for {ticker}")
            return None
            
        latest = df.iloc[-1]
        return {
            "ticker": ticker,
            "name": ticker_info["name"],
            "datetime": str(latest.name),
            "open": float(latest["Open"].iloc[0]) if hasattr(latest["Open"], 'iloc') else float(latest["Open"]),
            "high": float(latest["High"].iloc[0]) if hasattr(latest["High"], 'iloc') else float(latest["High"]),
            "low": float(latest["Low"].iloc[0]) if hasattr(latest["Low"], 'iloc') else float(latest["Low"]),
            "close": float(latest["Close"].iloc[0]) if hasattr(latest["Close"], 'iloc') else float(latest["Close"]),
            "volume": int(latest["Volume"].iloc[0]) if hasattr(latest["Volume"], 'iloc') else int(latest["Volume"])
        }
    except Exception as e:
        print(f"Error fetching {ticker}: {str(e)}")
        return None

def process_ticker(ticker_info):
    """Xử lý từng ticker và gửi dữ liệu"""
    data = fetch_latest(ticker_info)
    if data:
        producer.send("stock_prices", value=data)
        return f"Sent: {ticker_info['symbol']}"
    return None

if __name__ == "__main__":
    # Giới hạn số luồng để tránh quá tải
    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(5):  # Lặp 5 lần
            print(f"\nBatch {i+1}:")
            futures = [executor.submit(process_ticker, ticker) for ticker in TICKERS]
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        print(result)
                except Exception as e:
                    print(f"Processing error: {str(e)}")
            
            # Đợi 60 giây giữa các batch
            if i < 4:  # Không đợi sau batch cuối
                time.sleep(60)
    
    # Đảm bảo tất cả message được gửi
    producer.flush()
    print("\nCompleted all batches")