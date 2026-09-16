"""Q9-(2) Spark 집계 잡 — netflix_titles.csv 를 type × 장르 별로 집계한다.

처리 내용
  - release_year 가 기준 연도 이상인 행만 남긴다.
    기준 연도는 스크립트 인자로 주입하며 기본값은 2015 이다. (하드코딩하지 않는다)
  - listed_in 컬럼은 쉼표로 구분된 장르 목록이므로,
    장르 하나당 한 행이 되도록 펼치고 앞뒤 공백을 제거한다.
  - type × 장르 별 작품 수를 집계한다.
  - 결과를 parquet(snappy 압축) 으로 저장한다.
  - 집계 행 수를 출력한다.

사용법
    spark-submit transform.py \
        --input  /opt/airflow/data/netflix_titles.csv \
        --output /opt/airflow/data/q9_silver/2026-09-15 \
        --base-year 2015
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Netflix titles type × genre 집계")
    parser.add_argument(
        "--input",
        default="/opt/airflow/data/netflix_titles.csv",
        help="입력 CSV 경로",
    )
    parser.add_argument(
        "--output",
        default="/opt/airflow/data/q9_silver/latest",
        help="parquet 출력 디렉터리",
    )
    parser.add_argument(
        "--base-year",
        type=int,
        default=2015,                      # ← 기본값 2015, 인자로 덮어쓸 수 있다
        help="이 연도 이상인 행만 남긴다 (기본값 2015)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("NetflixTransform_정혜정")
        # 결과 parquet 압축 코덱을 snappy 로 명시
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    print(f"[input]      {args.input}")
    print(f"[output]     {args.output}")
    print(f"[base_year]  {args.base_year}")

    # Netflix CSV 는 따옴표 안에 쉼표와 줄바꿈이 들어 있으므로 multiLine 으로 읽는다.
    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.input)
    )

    print(f"[input rows] {df.count()}")

    # 1) release_year 가 기준 연도 이상인 행만
    filtered = df.filter(F.col("release_year").cast("int") >= F.lit(args.base_year))

    # 2) listed_in 을 쉼표로 잘라 장르 하나당 한 행으로 펼치고 앞뒤 공백 제거
    exploded = (
        filtered
        .withColumn("genre", F.explode(F.split(F.col("listed_in"), ",")))
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre") != "")
    )

    # 3) type × 장르 별 작품 수 집계
    agg = (
        exploded
        .groupBy("type", "genre")
        .agg(F.count("*").alias("title_count"))
        .orderBy(F.col("type").asc(), F.col("title_count").desc(), F.col("genre").asc())
    )

    agg.show(200, truncate=False)

    # 4) parquet(snappy) 로 저장
    agg.write.mode("overwrite").option("compression", "snappy").parquet(args.output)
    print(f"[write]      parquet(snappy) -> {args.output}")

    # 5) 집계 행 수 출력 (채점 기준 값)
    row_count = agg.count()
    print(f"[agg rows]   {row_count}")

    spark.stop()


if __name__ == "__main__":
    main()
