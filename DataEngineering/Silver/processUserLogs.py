from Silver.process import Process
from pyspark.sql.functions import *

class ProcessUserLogs(Process):

    def renameColumns(self, df):
        return df.withColumnRenamed("msno", "user_id")

    def formatDates(self, df):
        return df.withColumn("date", to_date(col("date").cast("string"), "yyyyMMdd"))

    def filterInvalidSecs(self, df): #Remove rows with negative total_secs
        return df.filter("total_secs >= 0")
    
    def run(self):
        df = self.readFromBronze()
        df = self.renameColumns(df)
        df = self.formatDates(df)
        df = self.filterInvalidSecs(df)
        self.writeToSilver(df)

