"""Q8-(4) boto3 로 S3 에서 읽어오기.

하나의 스크립트에서 아래를 순서대로 수행한다.
  1. bronze/ 아래 객체 목록과 각 객체의 크기 출력
  2. netflix_titles.csv 를 project/data/ 아래로 다운로드
  3. 내려받은 파일의 행 수 출력 (CSV 레코드 수 기준 — 헤더 제외,
     따옴표로 묶인 값 안의 줄바꿈은 한 행으로 세지 않는다)

자격증명은 코드에 적지 않는다.
  - `aws configure` 로 저장한 IAM 사용자 프로파일(~/.aws/credentials) 을
    boto3 기본 자격증명 체인이 알아서 읽는다.
버킷 이름도 코드에 박지 않고 환경변수 또는 실행 인자로 받는다.

사용법
    export S3_BUCKET=de-3-jeonghyejeong
    python s3_download.py
  또는
    python s3_download.py de-3-jeonghyejeong
"""

from __future__ import annotations

import csv
import os
import sys

import boto3

PREFIX = "bronze/"
OBJECT_KEY = "bronze/netflix_titles.csv"
LOCAL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
REGION = os.environ.get("AWS_REGION", "ap-northeast-2")


def resolve_bucket() -> str:
    """버킷 이름을 실행 인자 → 환경변수 순서로 찾는다. (코드에 박지 않는다)"""
    if len(sys.argv) > 1:
        return sys.argv[1]

    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit(
            "버킷 이름이 없습니다. "
            "`export S3_BUCKET=de-3-jeonghyejeong` 또는 "
            "`python s3_download.py de-3-jeonghyejeong` 형태로 지정하세요."
        )
    return bucket


def list_objects(s3, bucket: str) -> None:
    """[1] bronze/ 아래 객체 목록과 크기 출력."""
    print(f"[1] list     s3://{bucket}/{PREFIX}")

    paginator = s3.get_paginator("list_objects_v2")
    found = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            found += 1
            print(f"    {obj['Key']:<45}{obj['Size']:>15,} bytes")

    if found == 0:
        raise SystemExit(f"s3://{bucket}/{PREFIX} 아래에 객체가 없습니다.")


def download(s3, bucket: str) -> str:
    """[2] netflix_titles.csv 를 project/data/ 아래로 다운로드."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    local_path = os.path.join(LOCAL_DIR, os.path.basename(OBJECT_KEY))

    print(f"[2] download s3://{bucket}/{OBJECT_KEY} -> data/{os.path.basename(OBJECT_KEY)}")
    s3.download_file(bucket, OBJECT_KEY, local_path)

    size = os.path.getsize(local_path)
    print(f"    downloaded ({size:,} bytes)")
    return local_path


def count_csv_records(local_path: str) -> int:
    """[3] CSV 레코드 수를 센다. 헤더 제외, 따옴표 안의 줄바꿈은 한 행으로 치지 않는다."""
    # 큰 필드가 들어 있어도 파싱이 끊기지 않도록 상한을 올려 둔다.
    csv.field_size_limit(sys.maxsize)

    with open(local_path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.reader(fp)
        header = next(reader, None)
        if header is None:
            return 0
        return sum(1 for _ in reader)


def main() -> None:
    bucket = resolve_bucket()
    s3 = boto3.client("s3", region_name=REGION)

    list_objects(s3, bucket)
    local_path = download(s3, bucket)

    rows = count_csv_records(local_path)
    print(f"[3] rows     {rows:,}")


if __name__ == "__main__":
    main()
