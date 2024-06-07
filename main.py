from pyspark.sql import SparkSession
from Silver import processMembers, processTransactions, processUserLogs, processTrainData, processTestData
from Gold import goldView

if __name__ == '__main__':

    spark = SparkSession.builder.appName("KKBox").getOrCreate()
    bronzeBucket = "gs://mack-churn-lake-bronze"
    silverBucket = "gs://mack-churn-lake-silver"
    goldBucket = "gs://mack-churn-lake-gold"

    #Silver
    processMembers.ProcessMembers(spark,bronzeBucket + "/members/*",silverBucket + "/members").run()
    processTransactions.ProcessTransactions(spark,bronzeBucket + "/transactions/*",silverBucket + "/transactions").run()
    processUserLogs.ProcessUserLogs(spark,bronzeBucket + "/userLogs/*",silverBucket + "/userLogs").run()
    processTrainData.ProcessTrainData(spark,bronzeBucket + "/trainData/*",silverBucket + "/trainData").run()
    processTestData.ProcessTestData(spark,bronzeBucket + "/testData/*",silverBucket + "/testData").run()

    #Gold
    goldView.GoldView(spark,silverBucket, goldBucket + "/GoldView").run()
