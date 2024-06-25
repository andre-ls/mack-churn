class View:
    def __init__(self, spark, silverDirectory, goldDirectory):
        self.spark = spark
        self.silverDirectory = silverDirectory
        self.goldDirectory = goldDirectory

    def readFromSilver(self, database):
        return self.spark.read.option("inferSchema","true").parquet(self.silverDirectory + "/" + database)

    def writeToGold(self, df):
        df.write.mode("overwrite").parquet(self.goldDirectory)
    
    def view(self, df):
        df.describe().show()
        print("-"*50)
        df.printSchema()
        print("-"*50)
        df.show()
