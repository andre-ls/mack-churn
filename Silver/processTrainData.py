from Silver.process import Process
from pyspark.sql.functions import *

class ProcessTrainData(Process):

    def renameColumns(self, df):
        return df.withColumnRenamed("msno", "user_id")

    def formatBoolean(self, df):
        return df.withColumn("is_churn", col("is_churn").cast("boolean"))

    def run(self):
        df = self.readFromBronze()
        df = self.renameColumns(df)
        df = self.formatBoolean(df)
        self.writeToSilver(df)
