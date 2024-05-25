/opt/bitnami/spark/bin/spark-submit \
--driver-memory 14g \
--jars /opt/bitnami/spark/yelp/jars/gcs-connector-hadoop3-latest.jar \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
main.py
