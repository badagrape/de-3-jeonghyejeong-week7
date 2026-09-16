"""Q4-(2) Spark Standalone 클러스터에서 실행하는 단어 빈도 집계.

조건
  - 입력은 제공된 data/wordcount.txt
  - 처리 흐름 : 텍스트 파일 읽기 → 공백 기준으로 단어 분리 → 단어별 개수 집계
                → 빈도 내림차순 상위 20 개 show()
  - 분리 규칙은 공백뿐 — 소문자 변환 · 구두점 제거를 하지 않는다.
    (Lorem 과 lorem, amet 과 amet, 는 서로 다른 단어로 센다)

실행 (컨테이너 안에서 spark-submit 으로 제출)
    docker compose -f docker-compose.spark.yaml exec spark-master \
      /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --name WordCount_정혜정 \
        /opt/spark-jobs/wordcount.py /opt/spark-data/wordcount.txt
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_INPUT = "/opt/spark-data/wordcount.txt"
TOP_N = 20


def main() -> None:
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT

    spark = (
        SparkSession.builder
        .appName("WordCount_정혜정")
        .getOrCreate()
    )

    # 1) 텍스트 파일 읽기 — 한 줄이 한 행("value" 컬럼)
    lines = spark.read.text(input_path)

    # 2) 공백 기준으로 단어 분리
    #    \s+ 로 잘라 연속 공백을 하나로 보고, 잘린 뒤 남는 빈 문자열만 제거한다.
    #    (소문자 변환 · 구두점 제거는 하지 않는다)
    words = (
        lines
        .select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
        .filter(F.col("word") != "")
    )

    # 3) 단어별 개수 집계
    counts = words.groupBy("word").agg(F.count("*").alias("count"))

    # 4) 빈도 내림차순 상위 20 개
    #    같은 빈도끼리는 단어 오름차순으로 정렬해 결과를 결정적으로 만든다.
    top_words = counts.orderBy(F.col("count").desc(), F.col("word").asc())

    total_words = words.count()
    distinct_words = counts.count()
    print(f"[input] {input_path}")
    print(f"[total words] {total_words}")
    print(f"[distinct words] {distinct_words}")
    print(f"[top {TOP_N} words]")

    top_words.show(TOP_N, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
