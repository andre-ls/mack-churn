from Gold.view import View
from pyspark.sql.functions import *

class TransactionsTimeView(View):

    def aggregate(self, df):
        return df.groupBy('transaction_date').agg(
            sum('actual_amount_paid').alias('total_amount_paid'),
            count('user_id').alias('total_transactions'),
            count_if('is_cancel').alias('total_canceling_transactions')
        )

    def aggregatePaymentMethod(self, df):
        return df.groupBy('transaction_date').pivot('payment_method_id').count().na.fill(0)

    def run(self):
        df = self.readFromSilver()
        df_user = self.aggregate(df)
        df_payment_method = self.aggregatePaymentMethod(df)
        df_view = df_user.join(df_payment_method, on='transaction_date', how='inner')
        self.writeToGold(df_view)
