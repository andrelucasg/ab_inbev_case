FROM apache/airflow:2.9.3

USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install delta-spark==2.4.0 azure-storage-blob azure-storage-file-datalake pyspark==3.4.3 geopy

USER root
RUN mkdir -p /usr/local/spark/jars
WORKDIR /usr/local/spark/jars
RUN wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -O delta-core.jar && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -O delta-storage.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.4.0/hadoop-azure-3.4.0.jar -O hadoop-azure.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.4.0/hadoop-azure-datalake-3.4.0.jar -O hadoop-azure-datalake.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.4.0/hadoop-client-3.4.0.jar -O hadoop-client.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.0/hadoop-common-3.4.0.jar -O hadoop-common.jar && \
    wget https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.13.14/scala-library-2.13.14.jar -O scala-library.jar && \
    wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar -O azure-storage.jar && \
    wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.48.v20220622/jetty-util-ajax-9.4.48.v20220622.jar -O jetty-util-ajax.jar && \
    wget https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util/9.4.48.v20220622/jetty-util-9.4.48.v20220622.jar -O jetty-util.jar

USER airflow
RUN mkdir -p $AIRFLOW_HOME/spark/conf
RUN echo "spark.jars /usr/local/spark/jars/delta-core.jar,/usr/local/spark/jars/hadoop-azure.jar,/usr/local/spark/jars/delta-storage.jar,/usr/local/spark/jars/hadoop-client.jar,/usr/local/spark/jars/hadoop-common.jar,/usr/local/spark/jars/hadoop-azure-datalake.jar,/usr/local/spark/jars/scala-library.jar,/usr/local/spark/jars/azure-storage.jar,/usr/local/spark/jars/jetty-util-ajax.jar,/usr/local/spark/jars/jetty-util.jar" > $AIRFLOW_HOME/spark/conf/spark-defaults.conf