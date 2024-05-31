from Silver.process import Process
from pyspark.sql.functions import *

class ProcessTestData(Process):

    def dropColumn(self, df):
        return df.drop("is_churn")

    def renameColumn(self, df):
        return df.withColumnRenamed("msno", "user_id")

    def run(self):
        df = self.readFromBronze()
        df = self.dropColumn(df)
        df = self.renameColumn(df)
        self.writeToSilver(df)
