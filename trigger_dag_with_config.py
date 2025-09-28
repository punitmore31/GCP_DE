import subprocess
import requests
import json


TABLE_LIST = ["MANKELI_OWNER.DIM_PROFIT_LOSS_HEADER","MANKELI_OWNER.DIM_PROFIT_LOSS_PARENT_CHILD", "MANKELI_OWNER.DIM_TELCO_DS"]

# ---------------- CONFIG ----------------
GCLOUD_PATH = r"C:\Users\punitkumar.more\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd"
AIRFLOW_URI = "https://f8e1cc9d516641158b00f03c93e39f3b-dot-europe-west1.composer.googleusercontent.com"
DAG_ID = "oracle_history_load_dynamic_dag_dif"
BATCH_BQ_DATASET = "FDW_HISTORICAL_STAGE"
TASK_PROP_FILE_PREFIX = "oracle_" 
# ----------------------------------------

token = subprocess.check_output(
    [GCLOUD_PATH, "auth", "print-access-token"], text=True).strip()
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

for idx, full_table_name in enumerate(TABLE_LIST):
    final_dataset, final_table = full_table_name.split(".")
    bq_dataset_name = BATCH_BQ_DATASET
    bq_table_name = final_table
    final_bq_dataset_name = final_dataset
    final_bq_table_name = final_table
    task_property_file_name = f"{TASK_PROP_FILE_PREFIX}{bq_table_name.lower()}_task_prop"

    dag_conf = {
        "bq_dataset_name": bq_dataset_name,
        "bq_table_name": bq_table_name,
        "final_bq_dataset_name": final_bq_dataset_name,
        "final_bq_table_name": final_bq_table_name,
        "task_property_file_name": task_property_file_name
    }

    print(f"## {idx+1} ::: {full_table_name} with conf: {dag_conf}")

    trigger_url = f"{AIRFLOW_URI}/api/v1/dags/{DAG_ID}/dagRuns"
    payload = {
        "conf": dag_conf
    }

    response = requests.post(trigger_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200 or response.status_code == 201:
        print(f"Successfully triggered DAG for {full_table_name}")
    else:
        print(f"Failed to trigger DAG for {full_table_name}, status: {response.status_code}, response: {response.text}")

    print("# =================================================== #\n")


