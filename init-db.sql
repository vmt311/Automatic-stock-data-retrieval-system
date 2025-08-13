-- Tạo bảng stock_prices nếu chưa tồn tại
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(50),
    datetime TIMESTAMP NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT,
    processed_at TIMESTAMP,
    CONSTRAINT unique_ticker_datetime UNIQUE (ticker, datetime)
);

-- Tạo index để tối ưu query
CREATE INDEX IF NOT EXISTS idx_ticker ON stock_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_datetime ON stock_prices(datetime);

-- Tạo bảng audit log (nếu cần)
CREATE TABLE IF NOT EXISTS etl_audit_log (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    records_processed INT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    error_message TEXT
);