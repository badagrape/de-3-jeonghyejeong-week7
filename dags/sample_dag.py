"""Q3-(2) 기본 DAG — 4 개 task 를 순차로 연결.

조건
  - 시작 → 파이썬 작업 2 개 → 종료 의 4 개 task 를 순차 연결
  - 시작 · 종료는 아무 동작도 하지 않는 task (EmptyOperator)
  - 파이썬 작업 2 개는 각각 문자열을 return  (print 가 아니라 반환값)
  - 스케줄은 매일 1 회, 과거 구간은 자동 실행되지 않도록 설정 (catchup=False)

반환값은 Task Log 의
    INFO - Done. Returned value was: ...
줄에서 확인할 수 있다.
"""

from __future__ import annotations

import pendulum
from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")


def say_hello() -> str:
    """문자열을 반환한다 (print 가 아니라 return)."""
    return "Hello Airflow"


def say_goodbye() -> str:
    """문자열을 반환한다 (print 가 아니라 return)."""
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    description="시작 → 파이썬 작업 2 개 → 종료 로 이어지는 기본 DAG",
    start_date=pendulum.datetime(2026, 9, 1, tz=KST),
    schedule="@daily",               # 매일 1 회
    catchup=False,                   # 과거 구간 자동 실행 안 함
    max_active_runs=1,
    tags=["q3", "sample"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello,
    )

    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=say_goodbye,
    )

    end = EmptyOperator(task_id="end")

    start >> hello_task >> goodbye_task >> end
