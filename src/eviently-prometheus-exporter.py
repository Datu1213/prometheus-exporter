from pyspark.sql import SparkSession

import numpy as np

import pandas as pd

from evidently import Report
from evidently.presets import DataDriftPreset

import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# Connect to Spark

spark = SparkSession.builder \
    .appName("mlflow") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "2") \
    .enableHiveSupport() \
    .getOrCreate()

silver_df = spark.table("default.stg_user_events")

# Trigger one Computing and save it to Spark memory
silver_df.cache()

# --------------------------------------------------------------

# --- 3. TransformSpark DataFrame ito Pandas DataFrame ---
# For TB Level data use .sample()
print("Transforming Spark DataFrame to Pandas DataFrame...")
all_data_pd = silver_df.toPandas()
print("Transforming Done.")

# --- 4. Imitate Reference data & Current_data ---
split_point = int(len(all_data_pd) * 0.5)
reference_data_pd = all_data_pd.iloc[:split_point]
current_data_pd = all_data_pd.iloc[split_point:]

print(f"Total len: {len(all_data_pd)}")
print(f"Reference data: {len(reference_data_pd)} 行")
print(f"Current_data: {len(current_data_pd)} 行")

print("\n正在生成数据漂移报告 (DataDriftPreset)...")

# DataDriftPreset 会自动分析所有列，包括我们的"ts"（时间戳）
# "purchase_value"（数值）和 "event_type"（分类）
data_drift_report = Report(metrics=[
    DataDriftPreset(),
])

# 运行计算！
my_report = data_drift_report.run(
    current_data=current_data_pd, 
    reference_data=reference_data_pd, 
)

# --- TODO: 6. Save report as HTML to S3 ---
# report_path = "/home/jovyan/work/data_drift_report.html"
# my_report.save_html(report_path)

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import json


# --- 2. Extract data From report ---

drift_score = 0.0
drift_detected = 0

try:
    # 
    for metric in my_report.dict().get("metrics", []):
        if 'DriftedColumnsCount' in metric.get("metric_id"):
            print("Find it")
            drift_score = metric.get("value", {}).get("share", 0.0)
            drift_detected = metric.get("value", {}).get("count", 0)
            break
except Exception as e:
    print(f"Can not extract drift score from Evidently report: {e}")

    # --- 3. Prepare indices ---
    registry = CollectorRegistry()
    g_drift_score = Gauge(
        'model_data_drift_score', 
        'Data drift score', 
        ['model_name'], 
        registry=registry
    )
    g_drift_detected = Gauge(
        'model_data_drift_detected', 
        'Clolumes numbers which are detected with drift', 
        ['model_name'], 
        registry=registry
    )

    # --- 4. Set indices value ---
    model_name = "stg_user_events_v1"
    g_drift_score.labels(model_name=model_name).set(drift_score)
    g_drift_detected.labels(model_name=model_name).set(drift_detected)

    # --- 5. Export to Pushgateway ---
    try:
        # User Docker network name 'pushgateway' & port '9091'
        push_to_gateway('pushgateway:9091', job='evidently_batch_validation', registry=registry)
        print(f"\n--- Done! ---")
        print(f"Exported data to Prometheus Pushgateway:")
        print(f"model_data_drift_score = {drift_score}")
        print(f"model_data_drift_detected = {drift_detected}")

    except Exception as e:
        print(f"\n--- Fail! ---")
        print(f"Can not export data to Prometheus Pushgateway (http://pushgateway:9091): {e}")
        print("Make sure Pushgateway server is runing and is with correct network address.")

print("\n--- Done! ---")