FROM apache/hive:3.1.3

ENV HADOOP_VERSION=3.1.0
ENV AWS_SDK_VERSION=1.12.552

USER root
# Add hadoop-aws and aws-sdk
RUN apt-get -y update && apt-get install -y \
    wget

# Add hadoop-aws and aws-sdk
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/hive/lib/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /opt/hive/lib/

USER hive
ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]