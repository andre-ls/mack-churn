from pyspark.sql import SparkSession
from Silver import processMembers

if __name__ == '__main__':

    spark = SparkSession.builder.appName("KKBox").getOrCreate()
    bucketUrl = "gs://mack-churn-lake"

    #Silver
    processMembers.ProcessMembers(spark,bucketUrl + "/Bronze/members_v3.csv",bucketUrl + "/Silver/members").run()
