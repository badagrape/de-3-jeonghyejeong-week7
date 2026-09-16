"""Q6 — XCom 전달 + 재시도.

조건
  (1) 앞 작업이 값을 계산해 return  → XCom(return_value) 에 저장
      뒤 작업이 xcom_pull 로 받아 로그에 출력
  (3) 앞 작업에 retries(2 회 이상) 와 retry_delay 를 설정하고,
      첫 시도에서만 실패하게 만들어 재시도로 성공하게 한다.

첫 시도 판별은 실행(run) 단위 표식 파일로 한다.
표식이 없으면 = 첫 시도이므로 표식을 남기고 실패, 재시도에서는 표식이 있으므로 정상 수행.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")

TARGET_FILE = "/opt/airflow/data/wordcount.txt"
MARKER_DIR = "/opt/airflow/data/.q6_markers"


def count_lines(**context) -> int:
    """파일의 줄 수를 세어 return 한다. 이번 실행의 첫 시도에서는 일부러 실패한다."""
    ti = context["ti"]
    run_id = context["run_id"]
    print(f"[attempt] run_id = {run_id}, try_number = {getattr(ti, 'try_number', 'n/a')}")

    os.makedirs(MARKER_DIR, exist_ok=True)
    safe = "".join(c if c.isalnum() else "_" for c in run_id)
    marker = os.path.join(MARKER_DIR, safe)

    if not os.path.exists(marker):
        with open(marker, "w", encoding="utf-8") as fp:
            fp.write("first attempt failed\n")
        raise AirflowException(
            "의도적으로 발생시킨 첫 시도 실패입니다. 재시도에서는 성공합니다."
        )

    path = TARGET_FILE if os.path.exists(TARGET_FILE) else __file__
    with open(path, "r", encoding="utf-8") as fp:
        line_count = sum(1 for _ in fp)

    print(f"[count_lines] {path} 의 줄 수 = {line_count}")
    return line_count


def use_value(**context) -> None:
    """앞 작업의 반환값을 xcom_pull 로 받아서 쓴다."""
    ti = context["ti"]
    value = ti.xcom_pull(task_ids="count_lines")
    print(f"[use_value] received from XCom: {value}")

    if value is None:
        raise AirflowException("XCom 에서 값을 받지 못했습니다.")

    print(f"[use_value] doubled = {value * 2}")


with DAG(
    dag_id="xcom_demo_정혜정",
    description="XCom 으로 값을 넘기고, 첫 시도 실패 후 재시도로 성공하는 DAG",
    start_date=pendulum.datetime(2026, 9, 1, tz=KST),
    schedule=None,
    catchup=False,
    tags=["q6", "xcom", "retry"],
) as dag:

    count_lines_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_lines_task >> use_value_task
