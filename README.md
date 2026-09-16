# 데이터 엔지니어링 7 주차 과제

- 이름: 정혜정
- 기수: 데엔 3 기
- 레포지토리 구성: Airflow(Docker Compose) + Spark Standalone + AWS S3 통합 파이프라인

## 폴더 구조

```
.
├── docker-compose.yaml          # Q3 공식 compose (Q5 에서 커스텀 이미지 빌드로 전환)
├── docker-compose.spark.yaml    # Q4 Spark Standalone 클러스터
├── Dockerfile                   # Q5 커스텀 Airflow 이미지
├── requirements.txt             # Q5 추가 파이썬 패키지
├── .env                         # Airflow UID / 이미지 이름 (키는 넣지 않음)
├── s3_download.py               # Q8 boto3 S3 다운로드 스크립트
├── dags/
│   ├── sample_dag.py            # Q3
│   ├── xcom_demo_정혜정.py       # Q6
│   ├── backfill_demo_정혜정.py   # Q7
│   ├── weekly_pipeline_정혜정.py # Q9
│   └── jobs/transform.py        # Q9 Spark 집계 잡
└── jobs/
    └── wordcount.py             # Q4 단어 빈도 집계 잡
```

## 실행 순서

1. `docker compose up -d` — Airflow 기동 (UI: http://localhost:8080)
2. `docker compose -f docker-compose.spark.yaml up -d` — Spark 클러스터 기동 (UI: http://localhost:8081)
3. Airflow UI ▸ Admin ▸ Connections / Variables 등록 후 각 DAG 트리거

## 실습 환경

이번 주차 실습은 Docker Compose 로 Airflow 와 Spark Standalone 클러스터를 한 대의 머신에 띄워 진행했다.

## 회고

이번 주차에는 Airflow 와 Spark, S3 를 하나의 파이프라인으로 잇는 과정을 익혔다.
