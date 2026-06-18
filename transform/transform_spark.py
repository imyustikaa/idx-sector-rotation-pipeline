import logging
import psycopg2
import re
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as  F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)


# ======================== Configuration ========================
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"
RAW_BUCKET        = "idx-raw"

PG_URL = "jdbc:postgresql://postgres-warehouse:5432/idx_warehouse"
PG_PROPS = {
  "user":     "warehouse",
  "password": "warehouse",
  "driver":   "org.postgresql.Driver"
}

SECTORS = [
  "Finance", "Infrastructure", "Basic", "Energy", 
  "NonCyclicals", "Industrials", "Technology", "Healthcare", 
  "Property", "Cyclicals", "Transport"
]

PRICE_FIELDS = ["close", "high", "low", "open", "volume"]


# ======================== Spark Session ========================
def create_spark_session() -> SparkSession:
  return ( SparkSession.builder
    .appName("IDXSectorRotation_Transform")
    #.master("spark://spark-master:7077")
    .config("spark.executor.memory", "512m")
    .config("spark.driver.memory", "512m")
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
  )
  

# ======================= Read from MinIO =======================
def read_sector_parquet(spark: SparkSession, week_label:str, sector:str) -> DataFrame:
  path = f"s3a://{RAW_BUCKET}/daily/{week_label}/idx_sector/{sector}.parquet"
  try:
    df = spark.read.parquet(path)
    log.info(f"Read {sector}: {df.count()} rows")
    return df
  except Exception as e:
    log.warning(f"✘ Could not read {sector}: {e}")
    return spark.createDataFrame([], schema="date DATE, ticker STRING, sector STRING, close DOUBLE, volume LONG")


def clean_column_names(df: DataFrame) -> DataFrame:
  new_cols = []
  for col in df.columns:
    clean_name = (col.replace("('", "")
                    .replace("', '", "_")
                    .replace("', '')", "")
                    .replace("')", "")
                    .lower()
                    .replace("^jkse", "")
                    .strip("_ ")
                    .replace(".", "_"))
    if not clean_name:
      clean_name = "index"
    new_cols.append(clean_name)
  return df.toDF(*new_cols)


def unpivot_sector_df(df: DataFrame, sector: str) -> DataFrame:
  tickers = set()
  pattern = re.compile(r"^(close|high|low|open|volume)_(.+)$")
  for col in df.columns:
    m = pattern.match(col)
    if m:
      tickers.add(m.group(2))
    
  if not tickers:
    raise ValueError(f"No ticker column detected for {sector} | columns: {df.columns}")
  
  per_ticker_df = []
  for ticker in sorted(tickers):
    select_exprs = [F.col("date")]
    for field in PRICE_FIELDS:
      col_name = f"{field}_{ticker}"
      if col_name in df.columns:
        target_type = "long" if field == "volume" else "double"
        select_exprs.append(F.col(col_name).cast(target_type).alias(field))
      else:
        log.warning(f"✘[{sector}] No {col_name} founded")
        target_type = "long" if field == "volume" else "double"
        select_exprs.append(F.lit(None).cast(target_type).alias(field))

    upper_ticker = ticker.upper()
    display_ticker = upper_ticker[:-3] + ".JK" if upper_ticker.endswith("_JK") else upper_ticker
      
    ticker_df = (
      df.select(*select_exprs)
      .withColumn("ticker", F.lit(display_ticker))
      .withColumn("sector", F.lit(sector))
    )
    per_ticker_df.append(ticker_df)
  
  result = per_ticker_df[0]
  for d in per_ticker_df[1:]:
    result = result.union(d)
  
  return result

  
def read_benchmark_parquet(spark:SparkSession, week_label:str) -> DataFrame:
  path = f"s3a://{RAW_BUCKET}/daily/{week_label}/idx_sector/benchmark_IHSG.parquet"
  return spark.read.parquet(path)


# ===================== Data Transformation ======================
def compute_metrics(df: DataFrame, benchmark_df: DataFrame) -> DataFrame:
  # calculate market return
  bench = benchmark_df.withColumn("market_return", F.log(F.col("close") / F.lag("close").over(Window.orderBy("date"))))
  df = df.withColumn("daily_return", F.log(F.col("close") / F.lag("close").over(Window.partitionBy("ticker").orderBy("date"))))
  
  df = df.join(bench.select("date", "market_return"), on="date", how="left")
  
  w_ticker = Window.partitionBy("ticker").orderBy("date")
  w_short = Window.partitionBy("ticker").orderBy("date").rowsBetween(-63, 0)
  w_long = Window.partitionBy("ticker").orderBy("date").rowsBetween(-252, 0)
  
  # calculate metrics
  df = df \
      .withColumn("price_1m_ago", F.lag("close", 21).over(w_ticker)) \
      .withColumn("price_1y_ago", F.lag("close", 252).over(w_ticker)) \
      .withColumn("price_momentum", (F.col("price_1m_ago") / F.col("price_1y_ago")) - 1) \
      .withColumn("excess_return", F.col("daily_return") - 0.0001) \
      .withColumn("sharpe_ratio", (F.avg("excess_return").over(w_short) / F.stddev("daily_return").over(w_short))*F.sqrt(F.lit(252))) \
      .withColumn("rolling_beta", F.covar_samp("daily_return", "market_return").over(w_short) / F.var_samp("market_return").over(w_short)) \
      .withColumn("cum_ret", F.exp(F.sum("daily_return").over(w_ticker))) \
      .withColumn("rolling_max", F.max("cum_ret").over(w_long)) \
      .withColumn("max_drawdown", (F.col("cum_ret") - F.col("rolling_max")) / F.col("rolling_max"))
  return df


# ======================== Delete Old Data ========================
def delete_old_data(week_label: str):
  try:
    conn = psycopg2.connect(
      host="postgres-warehouse",
      database="idx_warehouse",
      user=PG_PROPS["user"], 
      password=PG_PROPS["password"]
    )
    cur = conn.cursor()
    log.info(f"Cleaning existing data for week: {week_label}")
    cur.execute(f"DELETE FROM bronze.sector_metrics WHERE week_label = '{week_label}'")
    cur.execute(f"DELETE FROM bronze.sector_prices WHERE week_label = '{week_label}'")
    cur.execute(f"DELETE FROM silver.ticker_metrics_daily WHERE week_label = '{week_label}'")
    conn.commit()
    cur.close()
    conn.close()
  except Exception as e:
    log.warning(f"Could not delete old data: {e}")


# ======================= Load to PostgreSQL =======================
def load_to_postgres(df: DataFrame, table: str, mode: str = "append") -> None:
  df.sparkSession.conf.set(
    "spark.sql.sources.partitionOverwriteMode", "dynamic"
  )
  df.write \
    .format("jdbc") \
    .option("url", PG_URL) \
    .option("dbtable", table) \
    .option("user", PG_PROPS["user"]) \
    .option("password", PG_PROPS["password"]) \
    .option("driver", PG_PROPS["driver"]) \
    .mode("overwrite") \
    .save()
  log.info(f"Loaded to {table}: {df.count()} rows")
  

# ======================= Main Entry Point =======================
def run_transform(week_label: str = None) -> None:
  if not week_label:
    week_label = datetime.today().strftime("%Y-W%W")
  
  log.info(f"Starting transform | week: {week_label}")
  spark = create_spark_session()
  delete_old_data(week_label)
  
  benchmark_df = read_benchmark_parquet(spark, week_label)
  benchmark_df = clean_column_names(benchmark_df)
  log.info(f"Benchmark columns after cleaning: {benchmark_df.columns}")
  
  all_sectors = []
  
  for sector in SECTORS:
    df = read_sector_parquet(spark, week_label, sector)
    if df.rdd.isEmpty():
      log.warning(f"Skipping empty sector: {sector}")
      continue
    
    df = clean_column_names(df)
    df = unpivot_sector_df(df, sector)
    df = compute_metrics(df, benchmark_df)
    all_sectors.append(df)    
    
  if not all_sectors:
    log.error("✘ No sector data available")
    return

  combined_df = all_sectors[0]
  for df in all_sectors[1:]:
    combined_df = combined_df.union(df)
    
  metrics_df = (
    combined_df
    .withColumn("week_label", F.lit(week_label))
    .withColumn("trade_date", F.col("date").cast("date"))
    .withColumn("loaded_at", F.current_timestamp())
    .fillna(0, subset=["daily_return"])
    .select(
      "trade_date", "week_label", "sector",
      "ticker", "daily_return", "loaded_at"
    )
  )
  load_to_postgres(metrics_df, "bronze.sector_metrics")
  
  price_df = (
    combined_df
    .withColumn("week_label", F.lit(week_label))
    .withColumn("trade_date", F.col("date").cast("date"))
    .withColumn("loaded_at", F.current_timestamp())
    .select(
      "trade_date", "week_label", "sector", "ticker",
      F.col("close").alias("close_price"),
      "volume", "loaded_at"
    )
  )
  load_to_postgres(price_df, "bronze.sector_prices")
  
  silver_df = (
    combined_df
    .withColumn("week_label", F.lit(week_label))
    .withColumn("trade_date", F.col("date").cast("date"))
    .withColumn("computed_at", F.current_timestamp())
    .fillna(0, subset=["price_momentum", "sharpe_ratio", "rolling_beta", "max_drawdown"])
    .select(
      "trade_date", "week_label", "sector", "ticker", 
      "price_momentum", "sharpe_ratio", "rolling_beta", "max_drawdown",
      "computed_at"
    )
  )
  load_to_postgres(silver_df, "silver.ticker_metrics_daily")
  
  log.info(f"Transform complete for week: {week_label}")


if __name__ == "__main__":
  run_transform()