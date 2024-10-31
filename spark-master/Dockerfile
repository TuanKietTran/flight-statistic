# Use the official Spark master image as the base
FROM bde2020/spark-master:latest

# Set environment variables
ENV INIT_DAEMON_STEP=setup_spark
ENV MINIO_ENDPOINT=http://minio:9000

# Create the jars directory
RUN mkdir -p /opt/spark/jars

# Download required JAR files into /opt/spark/jars
RUN curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar

# Expose the necessary ports
EXPOSE 7077 8080
