from pyspark.sql import SparkSession
from Silver import processMembers, processTransactions, processUserLogs

if __name__ == '__main__':

    spark = SparkSession.builder.appName("KKBox").getOrCreate()
    bucketUrl = "gs://mack-churn-lake"

    #Silver
    processMembers.ProcessMembers(spark,bucketUrl + "/Bronze/members_v3.csv",bucketUrl + "/Silver/members").run()
    processTransactions.ProcessTransactions(spark,bucketUrl + "/Bronze/transactions*.csv",bucketUrl + "/Silver/transactions").run()
    processUserLogs.ProcessUserLogs(spark,bucketUrl + "/Bronze/user_logs*.csv",bucketUrl + "/Silver/userLogs").run()
