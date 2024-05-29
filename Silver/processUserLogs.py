from Silver.process import Process
from pyspark.sql.functions import *

class ProcessUserLogs(Process):

    def renameColumns(self, df):
        return df.withColumnRenamed("msno", "user_id")

    def formatDates(self, df):
        return df.withColumn("date", to_date(col("date").cast("string"), "yyyyMMdd"))
    
    def run(self):
        df = self.readFromBronze()
        df = self.renameColumns(df)
        df = self.formatDates(df)
        self.writeToSilver(df)

