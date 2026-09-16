"""Q7 — Airflow 백필(catchup).

조건
  - 스케줄은 매일 1 회
  - start_date 를 작업하는 날 기준 7 일 전 날짜로 "고정된 날짜 리터럴" 로 적는다
  - DAG 를 켜거나 트리거했을 때 과거 구간의 실행을 자동으로 채우도록 설정 (catchup=True)
  - 각 실행이 자신의 logical date 에 해당하는 파일명으로 결과를 쓴다

★ 작업일이 다르면 START_DATE 한 줄만 바꾸면 된다. (작업일 - 7 일)
  예) 2026-09-15 에 진행 → 2026-09-08
"""

from __future__ import annotations

import os

import pendulum
from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")

# ★ 작업일(2026-09-16) 기준 7 일 전 — 고정된 날짜 리터럴
START_DATE = pendulum.datetime(2026, 9, 9, tz=KST)

# 실행별 산출물을 남길 경로 (컨테이너 안)
OUTPUT_DIR = "/opt/airflow/q7_output"


def write_daily_file(**context) -> str:
    """자신의 logical date(ds) 를 파일명에 넣어 결과를 쓴다."""
    ds = context["ds"]                      # 예: 2026-09-08
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    path = os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(f"logical_date={ds}\n")

    print(f"[write] {path}")
    return path


def verify_file(**context) -> int:
    """이번 실행이 만든 파일을 확인하고, 지금까지 쌓인 파일 목록을 로그로 남긴다."""
    ds = context["ds"]
    path = os.path.join(OUTPUT_DIR, f"daily_{ds}.txt")

    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} 가 만들어지지 않았습니다.")

    files = sorted(os.listdir(OUTPUT_DIR))
    print(f"[verify] {path} 확인 완료")
    print(f"[verify] {OUTPUT_DIR} 안의 파일 {len(files)} 개")
    for name in files:
        print(f"  - {name}")

    return len(files)


with DAG(
    dag_id="backfill_demo_정혜정",
    description="start_date 를 7 일 전으로 고정하고 catchup 으로 과거 구간을 채우는 DAG",
    start_date=START_DATE,
    schedule="0 0 * * *",            # 매일 1 회 (KST 00:00)
    catchup=True,                    # ★ 과거 구간의 실행을 자동으로 채운다
    max_active_runs=1,               # 과거 실행을 한 번에 하나씩 순서대로
    tags=["q7", "backfill", "catchup"],
) as dag:

    write = PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )

    verify = PythonOperator(
        task_id="verify_file",
        python_callable=verify_file,
    )

    write >> verify
