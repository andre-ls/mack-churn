class Process:
    def __init__(self, spark, bronzeDirectory, silverDirectory):
        self.spark = spark
        self.bronzeDirectory = bronzeDirectory
        self.silverDirectory = silverDirectory

    def readFromBronze(self):
        return self.spark.read.option("inferSchema","true").option("header","true").csv(self.bronzeDirectory)

    def writeToSilver(self, df):
        df.write.mode("overwrite").parquet(self.silverDirectory)
    
    def view(self, df):
        df.describe().show()
        print("-"*50)
        df.printSchema()
        print("-"*50)
        df.show()
