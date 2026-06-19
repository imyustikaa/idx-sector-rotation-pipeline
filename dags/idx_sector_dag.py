import subprocess
import sys
import requests
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable


# ========================= Default Arguments for DAG =========================
default_args = {
  "owner": "idxsectorrotation",
  "depends_on_past": False,
  "email_on_failure": False,
  "email_on_retry": False,
  "retries": 2,
  "retry_delay": timedelta(minutes=5),
}


# ========================= Install Python Dependencies =========================
# extract data & save to MinIO
def run_extract(**context):
  # extract data from yfinance
  sys.path.insert(0, "/opt/airflow/")
  from extract.extract_idx_sector import run_extraction
  
  week_label = context["ds_nodash"][:4] + "-W" + datetime.strptime(
    context["ds"], "%Y-%m-%d"
  ).strftime("%W")
  
  summary = run_extraction(week_label=week_label)
  
  context["ti"].xcom_push(key="week_label", value=week_label)
  context["ti"].xcom_push(key="extract_summary", value=summary)
  
  total_rows = sum(summary.values())
  print(f"✔ Extracted {total_rows} rows for week {week_label}")
  
  if total_rows == 0:
    raise ValueError("✘ No data extracted")
  

def run_spark_transform(**context):
  week_label = context["ti"].xcom_pull(task_ids="extract_data", key="week_label")
  
  sys.path.insert(0, "/opt/airflow")
  from transform.transform_spark import run_transform
  
  run_transform(week_label=week_label)
  print(f"Spark transform complete for week: {week_label}")


def run_dbt(**context):
  full_refresh = Variable.get("dbt_full_refresh", default_var="false").lower() == "true"

  cmd = ["dbt", "run",
  "--project-dir", "/opt/airflow/dbt_project",
  "--profiles-dir", "/opt/airflow/dbt_project"]

  if full_refresh:
    cmd.append("--full-refresh")
    log.info("Running dbt with --full-refresh")

  result = subprocess.run(cmd, capture_output=True, text=True,)
  print(result.stdout)
  if result.returncode != 0:
    print(result.stderr)
    raise RuntimeError(f"✘ Failed dbt run: \n{result.stderr}")
  print(f"✔ dbt run complete!")

def reset_full_refresh(**context):
  current = Variable.get("dbt_full_refresh", default_var="false")
  if current.lower() == "true":
    Variable.set("dbt_full_refresh", "false")
    print("✔ dbt_full_refresh reset to false")
  else:
    print("dbt_full_refresh: no reset needed")


def run_dbt_test(**context):
  result = subprocess.run(
    ["dbt", "test",
     "--project-dir", "/opt/airflow/dbt_project",
     "--profiles-dir", "/opt/airflow/dbt_project"],
    capture_output=True,
    text=True,
  )
  
  print(result.stdout)
  
  if result.returncode != 0:
    print(result.stderr)
    raise RuntimeError(f"✘ Failed dbt test: \n{result.stderr}")
  
  print(f"✔ dbt test passed: data quality validated")
  

def metabase_sync(**context):
  METABASE_URL = "http://metabase:3000"
  MB_USER      = Variable.get("metabase_admin")
  MB_PASS      = Variable.get("metabase_pass")
  MB_WAREHOUSE_DB_ID = 2
  MAX_READY_RETRIES = 10
  READY_INTERVAL = 15
  
  for attempt in range(1, MAX_READY_RETRIES+1):
    try:
      health = requests.get(f"{METABASE_URL}/api/health", timeout=10)
      if health.status_code == 200 and health.json().get("status") == "ok":
        print(f"✔ Metabase ready | attempt {attempt}")
        break
    except Exception as e:
      print(f"Metabase not ready yet... | attempt {attempt}")
    
    if attempt == MAX_READY_RETRIES:
      raise RuntimeError("✘ Metabase not ready")
    
    time.sleep(READY_INTERVAL)

  session = requests.post(
    f"{METABASE_URL}/api/session",
    json={"username": MB_USER, "password": MB_PASS},
    timeout=30,
  )
  session.raise_for_status()
  token = session.json()["id"]
  headers = {"X-Metabase-Session": token}
  
  sync = requests.post(
    f"{METABASE_URL}/api/database/{MB_WAREHOUSE_DB_ID}/sync_schema",
    headers=headers,
    timeout=30,
  )
  sync.raise_for_status()
  print(f"✔ Metabase sync_schema triggered (DB ID: {MB_WAREHOUSE_DB_ID})")
  
  time.sleep(30)

  rescan = requests.post(
    f"{METABASE_URL}/api/database/{MB_WAREHOUSE_DB_ID}/rescan_values",
    headers=headers,
    timeout=30,
  )
  rescan.raise_for_status()
  print("✔ Metabase rescan_values triggered.")


# =============================== DAG Definition ===============================
with DAG(
  dag_id            = "idxsector_rotation_weekly",
  default_args       = default_args,
  description       = "IDX Sector Rotation: Weekly Batch ETL Pipeline",
  schedule_interval = "0 0 * * 1",
  start_date        = datetime(2025, 1, 1),
  catchup           = False,
  max_active_runs   = 1,
  tags              = ["idxsectorrotation", "finance", "batch", "weekly"]
) as dag:
  
  # Task1: Fix dependencies & install dbt
  fix_dependencies = BashOperator(
  task_id = "upgrade_yfinance",
  bash_command = (
    "pip install --upgrade yfinance && " 
    "pip install --force-reinstall cffi==1.16.0 && "
    "pip install --quiet dbt-postgres"
    ),
  )
  
  # Task2: Extract
  t_extract = PythonOperator(
    task_id = "extract_data",
    python_callable = run_extract,
  )
  
  # Task3: Spark Transform
  t_transform = PythonOperator(
    task_id   = "transform_spark",
    python_callable = run_spark_transform,
  )
  
  # Task4: dbt run
  t_dbt_run = PythonOperator(
    task_id = "dbt_run",
    python_callable = run_dbt,
  )

  # Task5: reset flag
  t_reset_flag = PythonOperator(
    task_id = "reset_dbt_fullrefresh",
    python_callable=reset_full_refresh,
  )
  
  # Task6: dbt_test
  t_dbt_test = PythonOperator(
    task_id  = "dbt_test",
    python_callable = run_dbt_test,
  )
  
  # Task7: Metabase sync schema
  t_metabase_sync = PythonOperator(
    task_id = "metabase_sync",
    python_callable = metabase_sync,
    trigger_rule = TriggerRule.ALL_SUCCESS,
  )

  # ============================= Task Dependencies =============================
  (
    fix_dependencies
    >> t_extract
    >> t_transform
    >> t_dbt_run
    >> t_reset_flag
    >> t_dbt_test
    >> t_metabase_sync
  )