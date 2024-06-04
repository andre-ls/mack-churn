from Gold.view import View
from pyspark.sql.functions import *

class TransactionsView(View):

    def aggregate(self, df):
        return df.groupBy('user_id').agg(
            sum('actual_amount_paid').alias('total_amount_paid'),
            count('user_id').alias('total_transactions'),
            max('transaction_date').alias('last_transaction_date'),
            max('membership_expire_date').alias('last_membership_expire_date'),
            count_if('is_cancel').alias('total_cancelation_transactions')
        )

    def aggregatePaymentMethod(self, df):
        return df.groupBy('user_id').pivot('payment_method_id').count().na.fill(0)

    def run(self):
        df = self.readFromSilver()
        df_user = self.aggregate(df)
        df_payment_method = self.aggregatePaymentMethod(df)
        df_view = df_user.join(df_payment_method, on='user_id', how='inner')
        self.writeToGold(df_view)
