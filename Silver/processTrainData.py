from Silver.process import Process
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

class ProcessTrainData(Process):

    def readFromBronze(self):
        return self.spark.read.option("inferSchema","true")\
                              .option("header","true")\
                              .csv(self.bronzeDirectory)\
                              .withColumn("churn_month", input_file_name())\
                              .withColumn("churn_month", regexp_replace(col("churn_month"),"(\D)",""))\
                              .withColumn("churn_month", regexp_replace(col("churn_month") ,"(\\d{4})(\\d{2})" , "$1-$2"))
    
    def splitChurnMonth(self, df):
        return df.withColumn("year", split(col("churn_month"),"-")[0].cast(IntegerType()))\
                 .withColumn("month", split(col("churn_month"),"-")[1].cast(IntegerType()))\
                 .drop("churn_month")

    def renameColumns(self, df):
        return df.withColumnRenamed("msno", "user_id")

    def formatBoolean(self, df):
        return df.withColumn("is_churn", col("is_churn").cast("boolean"))

    def run(self):
        df = self.readFromBronze()
        df = self.splitChurnMonth(df)
        df = self.renameColumns(df)
        df = self.formatBoolean(df)
        self.writeToSilver(df)
