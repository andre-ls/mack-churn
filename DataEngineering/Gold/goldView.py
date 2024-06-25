from Gold.view import View
from pyspark.sql.functions import *

class GoldView(View):

    def createChurnStruct(self):
        trainDf = self.readFromSilver("trainData")
        structColumns = trainDf.columns
        structColumns.remove("user_id")
        churnDf = trainDf.withColumn("churn", struct(*structColumns))\
                                       .select("user_id", "churn")\
                                       .groupBy("user_id").agg(collect_list("churn").alias("churn"))
        return churnDf

    def createMembersView(self):
        membersDf = self.readFromSilver("members")
        churnDf = self.createChurnStruct()
        membersDf = membersDf.join(churnDf, how="left",on="user_id")
        return membersDf

    def createTransactionsStruct(self):
        transactionsDf = self.readFromSilver("transactions")
        structColumns = transactionsDf.columns
        structColumns.remove("user_id")
        transactionsDf = transactionsDf.withColumn("transactions", struct(*structColumns))\
                                       .select("user_id", "transactions")\
                                       .groupBy("user_id").agg(collect_list("transactions").alias("transactions"))
        return transactionsDf

    def createUserLogsStruct(self):
        userLogsDf = self.readFromSilver("userLogs")
        structColumns = userLogsDf.columns
        structColumns.remove("user_id")
        userLogsDf = userLogsDf.withColumn("user_logs", struct(*structColumns))\
                               .select("user_id", "user_logs")\
                               .groupBy("user_id").agg(collect_list("user_logs").alias("user_logs"))
        return userLogsDf

    def run(self):
        membersDf = self.createMembersView()
        transactionsDf = self.createTransactionsStruct()
        userLogsDf = self.createUserLogsStruct()
        goldDf = membersDf.join(transactionsDf, how="left", on="user_id")
        goldDf = goldDf.join(userLogsDf, how="left", on="user_id")
        self.writeToGold(goldDf)
