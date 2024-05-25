from Silver.process import Process
from pyspark.sql.functions import *

class ProcessMembers(Process):

    def renameColumns(self, df):
        return df.withColumnRenamed("bd", "age")\
                 .withColumnRenamed("msno", "user_id")

    def filterAge(self, df):
        return df.where("age >=0 and age <= 100")

    def filterGender(self, df):
        return df.where("gender == 'male' or gender == 'female'")

    def formatRegistrationInitTime(self, df):
        return df.withColumn("registration_init_time", to_date(col("registration_init_time").cast("string"), "yyyyMMdd"))

    def run(self):
        df = self.readFromBronze()
        df = self.renameColumns(df)
        df = self.filterAge(df)
        df = self.filterGender(df)
        df = self.formatRegistrationInitTime(df)
        self.writeToSilver(df)
