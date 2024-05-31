from Gold.view import View
from pyspark.sql.functions import *

class UserLogsView(View):

    def aggregate(self, df):
        return df.groupBy("user_id").agg(
            sum("num_25").alias("num_25"),\
            sum("num_50").alias("num_50"),\
            sum("num_75").alias("num_75"),\
            sum("num_985").alias("num_985"),\
            sum("num_100").alias("num_100"),\
            sum("num_unq").alias("num_unq"),\
            sum("total_secs").alias("total_secs")
        )

    def run(self):
        df = self.readFromSilver()
        df = self.aggregate(df)
        self.view(df)
        #self.writeToGold(df)
