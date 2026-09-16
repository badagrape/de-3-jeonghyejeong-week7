# Q5 — 커스텀 Airflow 이미지
#
# 빌드 결과 이미지 이름은 docker-compose.yaml 의 x-airflow-common ▸ image 에서 지정한다.
#   image: ${AIRFLOW_IMAGE_NAME:-week7-airflow:jeonghyejeong}
#   build: .
#
#   docker compose build
#   docker compose config        # 모든 Airflow 서비스가 커스텀 이미지로 바뀐 것 확인
#   docker images --digests
#
# 주의: FROM 의 태그는 Q3 에서 curl 로 내려받은 docker-compose.yaml 의
#       AIRFLOW_IMAGE_NAME 기본값과 같은 버전으로 맞춘다.

FROM apache/airflow:3.3.1

# ── (1) JDK 설치 ─────────────────────────────────────────────────────────────
# Spark(pyspark) 는 JVM 위에서 돌기 때문에 JDK 가 필요하다.
# apt 로 패키지를 설치하려면 root 권한이 있어야 한다.
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Debian 의 default-jdk 는 /usr/lib/jvm/default-java 심볼릭 링크를 만들어 준다.
# (아키텍처마다 실제 경로가 달라지므로 이 링크를 쓰는 편이 안전하다)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── (2) 파이썬 패키지 설치 ───────────────────────────────────────────────────
# pip 설치는 다시 airflow 유저로 돌아와서 수행한다.
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 설치 확인용 (빌드 로그에 버전이 찍힌다)
RUN java -version && python -c "import pyspark; print('pyspark', pyspark.__version__)"
