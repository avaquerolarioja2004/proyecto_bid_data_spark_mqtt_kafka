FROM apache/spark:3.5.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends sudo \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -s /bin/bash sparkuser

RUN pip install --no-cache-dir psycopg2-binary boto3

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN mkdir -p /tmp/.ivy2 \
    && chown -R sparkuser:sparkuser /tmp/.ivy2

COPY src /opt/urbanheat

RUN chown -R sparkuser:sparkuser /opt/urbanheat

USER sparkuser

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--conf", "spark.driver.host=urbanheat-spark", \
     "--conf", "spark.driver.bindAddress=0.0.0.0", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262", \
     "/opt/urbanheat/streaming.py"]