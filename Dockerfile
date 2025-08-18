FROM apache/spark:3.5.0

# Switch to root to create home directory
USER root

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download Hadoop AWS and AWS SDK jars
RUN pip install --no-cache-dir boto3 && \
    curl -L -o /tmp/hadoop-common-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar && \
    curl -L -o /tmp/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /tmp/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /tmp/postgresql-42.6.0.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar && \
    cp /tmp/*.jar /opt/spark/jars/ && \
    rm /tmp/*.jar && \
    chown -R spark:spark /opt/spark/jars

# Switch to the new user
USER spark

WORKDIR /home/spark