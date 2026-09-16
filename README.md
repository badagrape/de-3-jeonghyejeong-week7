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

Airflow 3.3.1 공식 compose 를 커스텀 이미지로 바꿔 띄웠고, Spark 는 master 1 대 + worker 2 대 Standalone 으로 구성했다. 전 과정을 macOS 한 대에서 진행했으며, 데이터는 S3 버킷 de-3-jeonghyejeong 을 사용했다.

## 회고

XCom 과 catchup 의 동작을 직접 눈으로 확인하면서 Airflow 스케줄링의 개념이 정리되었다. S3 → Spark → S3 로 이어지는 파이프라인을 DAG 하나로 묶으면서 task 를 분리하는 이유도 알게 되었다.
