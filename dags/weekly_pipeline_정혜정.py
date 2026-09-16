"""Q9 — Airflow 로 잇는 S3 · Spark 통합 파이프라인.

세 task 를 순서대로 연결한다. (한 task 안에서 전부 처리하지 않는다)
  1) download_csv  : boto3 로 s3://{버킷}/bronze/netflix_titles.csv 를 컨테이너 안으로
  2) transform     : spark-submit 으로 dags/jobs/transform.py 실행
  3) upload_silver : boto3 로 결과를 s3://{버킷}/silver/{오늘날짜}/ 에 올리고 목록·개수 출력

자격증명
  - 액세스 키는 코드 · yaml · .env 어디에도 적지 않는다.
  - S3 접근은 Airflow Connection `aws_default` 로만 한다.
    (S3Hook.get_conn() 이 돌려주는 boto3 client 를 사용)
버킷 이름
  - 환경변수 S3_BUCKET 또는 Airflow Variable `s3_bucket` 에서 받는다.
"""

from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

try:  # Airflow 3.x — task 코드에서는 Task SDK 의 Variable 을 쓴다
    from airflow.sdk import Variable
except ImportError:  # Airflow 2.x
    from airflow.models import Variable

try:  # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")

AWS_CONN_ID = "aws_default"
BRONZE_KEY = "bronze/netflix_titles.csv"
LOCAL_CSV = "/opt/airflow/data/netflix_titles.csv"
SILVER_LOCAL_ROOT = "/opt/airflow/data/q9_silver"
TRANSFORM_SCRIPT = "/opt/airflow/dags/jobs/transform.py"


def _get_variable(key: str, default=None):
    """Airflow 2.x(default_var) / 3.x(default) 의 인자 이름 차이를 흡수한다."""
    try:
        return Variable.get(key, default_var=default)
    except TypeError:
        return Variable.get(key, default=default)


def _bucket() -> str:
    """버킷 이름을 환경변수 → Airflow Variable 순서로 찾는다."""
    bucket = os.environ.get("S3_BUCKET") or _get_variable("s3_bucket", None)
    if not bucket:
        raise ValueError(
            "버킷 이름을 찾을 수 없습니다. "
            "환경변수 S3_BUCKET 또는 Airflow Variable `s3_bucket` 을 설정하세요."
        )
    return bucket


def download_csv(**context) -> str:
    """[1] boto3(S3Hook) 로 bronze/netflix_titles.csv 를 컨테이너 안으로 내려받는다."""
    bucket = _bucket()
    ti = context["ti"]

    # 오늘 날짜 — 뒤 task 들이 같은 값을 쓰도록 XCom 으로 넘긴다.
    run_date = pendulum.now(KST).to_date_string()          # 예: 2026-09-15
    ti.xcom_push(key="run_date", value=run_date)

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()        # boto3 S3 client
    os.makedirs(os.path.dirname(LOCAL_CSV), exist_ok=True)

    print(f"[download] s3://{bucket}/{BRONZE_KEY} -> {LOCAL_CSV}")
    s3.download_file(bucket, BRONZE_KEY, LOCAL_CSV)

    size = os.path.getsize(LOCAL_CSV)
    print(f"[download] done ({size:,} bytes), run_date = {run_date}")
    return LOCAL_CSV


def upload_silver(**context) -> int:
    """[3] 집계 결과 parquet 을 s3://{버킷}/silver/{오늘날짜}/ 로 올리고 목록·개수를 출력한다."""
    bucket = _bucket()
    ti = context["ti"]
    run_date = ti.xcom_pull(task_ids="download_csv", key="run_date")

    local_dir = os.path.join(SILVER_LOCAL_ROOT, run_date)
    prefix = f"silver/{run_date}/"

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()        # boto3 S3 client

    uploaded = 0
    for name in sorted(os.listdir(local_dir)):
        # Spark 가 남기는 _SUCCESS · .crc 등 부산물은 올리지 않는다.
        if not name.endswith(".parquet"):
            continue
        local_path = os.path.join(local_dir, name)
        key = prefix + name
        print(f"[upload] {local_path} -> s3://{bucket}/{key}")
        s3.upload_file(local_path, bucket, key)
        uploaded += 1

    if uploaded == 0:
        raise FileNotFoundError(f"{local_dir} 안에 올릴 parquet 파일이 없습니다.")

    # 업로드 결과 확인 — 목록과 개수 출력
    print(f"[list] s3://{bucket}/{prefix}")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])
    for obj in objects:
        print(f"    {obj['Key']:<70}{obj['Size']:>12,} bytes")
    print(f"[list] {len(objects)} objects under s3://{bucket}/{prefix}")

    return len(objects)


with DAG(
    dag_id="weekly_pipeline_정혜정",
    description="S3 다운로드 → Spark 집계 → S3 업로드 를 순서대로 잇는 파이프라인",
    start_date=pendulum.datetime(2026, 9, 1, tz=KST),
    schedule=None,                   # 수동 트리거
    catchup=False,
    max_active_runs=1,
    tags=["q9", "s3", "spark", "pipeline"],
) as dag:

    download_csv_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    # spark-submit 으로 집계 잡을 제출한다.
    # (SparkSubmitOperator 를 쓰려면 Spark Connection 이 필요하므로,
    #  여기서는 커스텀 이미지(Q5)에 설치된 pyspark 의 spark-submit 을 직접 호출한다)
    transform_task = BashOperator(
        task_id="transform",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            f"--name netflix_transform "
            f"{TRANSFORM_SCRIPT} "
            f"--input {LOCAL_CSV} "
            f"--output {SILVER_LOCAL_ROOT}/"
            "{{ ti.xcom_pull(task_ids='download_csv', key='run_date') }} "
            "--base-year {{ var.value.get('q9_base_year', '2015') }}"
        ),
    )

    upload_silver_task = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
    )

    download_csv_task >> transform_task >> upload_silver_task
