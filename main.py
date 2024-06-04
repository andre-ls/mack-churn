from pyspark.sql import SparkSession
from Silver import processMembers, processTransactions, processUserLogs, processTrainData, processTestData
from Gold import userLogsView, userLogsTimeView, transactionsView, transactionsTimeView, test

if __name__ == '__main__':

    spark = SparkSession.builder.appName("KKBox").getOrCreate()
    bucketUrl = "gs://mack-churn-lake"

    #Silver
    processMembers.ProcessMembers(spark,bucketUrl + "/Bronze/members_v3.csv",bucketUrl + "/Silver/members").run()
    processTransactions.ProcessTransactions(spark,bucketUrl + "/Bronze/transactions*.csv",bucketUrl + "/Silver/transactions").run()
    processUserLogs.ProcessUserLogs(spark,bucketUrl + "/Bronze/user_logs*.csv",bucketUrl + "/Silver/userLogs").run()
    processTrainData.ProcessTrainData(spark,bucketUrl + "/Bronze/train*.csv",bucketUrl + "/Silver/trainData").run()
    processTestData.ProcessTestData(spark,bucketUrl + "/Bronze/sample_submission_*.csv",bucketUrl + "/Silver/testData").run()

    #Gold
    transactionsView.TransactionsView(spark,bucketUrl + "/Silver/transactions",bucketUrl + "/Gold/transactionsView").run()
    transactionsTimeView.TransactionsTimeView(spark,bucketUrl + "/Silver/transactions",bucketUrl + "/Gold/transactionsTimeView").run()
    userLogsView.UserLogsView(spark,bucketUrl + "/Silver/userLogs",bucketUrl + "/Gold/userLogsView").run()
    userLogsTimeView.UserLogsTimeView(spark,bucketUrl + "/Silver/userLogs",bucketUrl + "/Gold/userLogsTimeView").run()
