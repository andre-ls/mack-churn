from Silver.process import Process
from pyspark.sql.functions import *

class ProcessTransactions(Process):

    def renameColumns(self, df):
        return df.withColumnRenamed("msno", "user_id")

    def formatDates(self, df):
        return df.withColumn("transaction_date", to_date(col("transaction_date").cast("string"), "yyyyMMdd"))\
                 .withColumn("membership_expire_date", to_date(col("membership_expire_date").cast("string"), "yyyyMMdd"))

    def formatBoolean(self, df):
        return df.withColumn("is_cancel", col("is_cancel").cast("boolean"))\
                 .withColumn("is_auto_renew", col("is_auto_renew").cast("boolean"))

    def run(self):
        df = self.readFromBronze()
        df = self.renameColumns(df)
        df = self.formatDates(df)
        df = self.formatBoolean(df)
        self.writeToSilver(df)
