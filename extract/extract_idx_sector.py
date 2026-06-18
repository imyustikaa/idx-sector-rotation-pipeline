import io
import logging
import time
import pandas as pd
import yfinance as yf
import requests
from minio import Minio
from minio.error import S3Error
from datetime import datetime, timedelta
from typing import Tuple, List
# from curl_cffi import requests as curl_requests

# yf.set_tz_cache_location("/opt/airflow/logs/yfinance_cache")
# session = requests.Session()
# session.headers.update({
#   "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
#   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#   "Accept-Language": "en-US,en;q=0.5",
# })

# ======================= Configurations =======================
# minio config
MINIO_ENDPOINT    = "minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"
RAW_BUCKET        = "idx-raw"

# 11 sector IDX tickers
SECTOR_TICKERS = {
  "Finance"       : ["BBCA.JK", "BBRI.JK", "BMRI.JK"],
  "Infrastructure": ["BREN.JK", "TLKM.JK", "TOWR.JK"],
  "Basic"         : ["TPIA.JK", "AMMN.JK", "BRPT.JK"],
  "Energy"        : ["DSSA.JK", "BYAN.JK", "ADRO.JK"],
  "NonCyclicals"  : ["AMRT.JK", "ICBP.JK", "INDF.JK"],
  "Industrials"   : ["ASII.JK", "UNTR.JK", "IMPC.JK"],
  "Technology"    : ["DCII.JK", "GOTO.JK", "EMTK.JK"],
  "Healthcare"    : ["KLBF.JK", "MIKA.JK", "SILO.JK"],
  "Property"      : ["PWON.JK", "BSDE.JK", "CTRA.JK"],
  "Cyclicals"     : ["MAPI.JK", "ACES.JK", "SCMA.JK"],
  "Transport"     : ["BIRD.JK", "SMDR.JK", "TMAS.JK"],
}

BENCHMARK_TICKER = "^JKSE"
HISTORICAL_YEARS = 3
MAX_RETRIES = 3
RETRY_DELAY = 5

# logging config
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s]: %(message)s"
)
log = logging.getLogger(__name__)


# ========================= MinIO Setup =========================
# init minio client
def get_minio_client() -> Minio:
  return Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
  )

# ensure bucket exists
def ensure_bucket(client: Minio, bucket: str) -> None:
  if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    log.info(f"✔ Bucket created: {bucket}")

# upload parquet to minio
def upload_parquet(client:Minio, df:pd.DataFrame, object_name:str) -> None:
  buffer = io.BytesIO()
  
  df.to_parquet(
    buffer,
    index=True,
    engine="pyarrow",
    version="2.0", 
    coerce_timestamps='us', 
    allow_truncated_timestamps=True
  )
  buffer.seek(0)
  size = buffer.getbuffer().nbytes
      
  client.put_object(
    RAW_BUCKET,
    object_name,
    data= buffer,
    length=size,
    content_type="application/octet-stream"
  )
  log.info(f"✔ Uploaded {object_name}({size/1024:.2f} KB)")


# ========================= Helpers =========================
# get historical data
def get_date_range() -> Tuple[str, str]:
  today = datetime.today()
  start = (today - timedelta(days=365*HISTORICAL_YEARS)).strftime("%Y-%m-%d")
  end = today.strftime("%Y-%m-%d")
  return start, end

# weekly label for object naming
def get_week_label() -> str:
  return datetime.today().strftime("%Y-%m-%d")

# download data with retry
def download_retry(ticker:str, start:str, end:str, retries:int = MAX_RETRIES) -> pd.DataFrame:
  for attempt in range(1, retries+1):
    # download data from yfinance
    try:
      df = yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=False
        # session=session
      )
      if not df.empty:
        if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
          df.index = df.index.tz_localize(None)
        return df
      log.error(f"✘ No data for {ticker} (attempt {attempt}/{retries})")
      
    except Exception as e:
      log.error(f"✘ Failed to download {ticker}: {e}")
    
    if attempt < retries:
      time.sleep(RETRY_DELAY)
  
  return pd.DataFrame()


# ========================= Data Extraction =========================
# extract sector data
def extract_sector(sector: str, tickers: List[str], start:str, end:str) -> pd.DataFrame:
  log.info(f"Extracting sector: {sector} | tickers: {tickers}")
  
  # process data for each ticker
  records = []
  
  for ticker in tickers:
    df = download_retry(ticker, start, end)
    
    if df.empty:
      log.warning(f"✘ No data for {ticker}")
      continue
    
    df = df.reset_index()
    df.columns = [str(col).lower() for col in df.columns]
    df["tickers"] = ticker
    df["sector"] = sector
    records.append(df)
    
    time.sleep(2)
    log.info(f"✔ Successfully extracted {ticker}")
  
  return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


# extract benchmark data
def extract_benchmark(ticker: str, start:str, end:str) -> pd.DataFrame:
  log.info(f"Extracting IHSG benchmark")
  df = download_retry(ticker, start, end)
    
  if df.empty:
    log.error("✘ No data for benchmark")
    return pd.DataFrame()
  
  df = df.reset_index()
  df.columns = [str(col).lower() for col in df.columns]
  df["ticker"] = ticker
  df["sector"] = "BENCHMARK"
  return df
  

# ========================= Main Execution =========================
def run_extraction(week_label: str = None) -> dict:
  if not week_label:
    week_label = get_week_label()
  
  start, end = get_date_range()
  log.info(f"Starting sector extraction | week: {week_label} | range: {start} → {end}")
  
  client = get_minio_client()
  ensure_bucket(client, RAW_BUCKET)
  
  summary = {}
  
  # extract each sector & upload to minio
  for sector, tickers in SECTOR_TICKERS.items():
    df = extract_sector(sector, tickers, start, end)
    if df.empty:
      summary[sector] = 0
      continue
    
    object_name = f"daily/{week_label}/idx_sector/{sector}.parquet"
    upload_parquet(client, df, object_name)
    summary[sector] = len(df)
  
  # extract benchmark & upload to minio  
  df_benchmark = extract_benchmark(BENCHMARK_TICKER, start, end)
  if not df_benchmark.empty:
    object_name = f"daily/{week_label}/idx_sector/benchmark_IHSG.parquet"
    upload_parquet(client, df_benchmark, object_name)
    summary["BENCHMARK"] = len(df_benchmark)
  
  log.info(f"✔ Extraction completed: {summary}")
  return summary


if __name__ == "__main__":
  run_extraction()