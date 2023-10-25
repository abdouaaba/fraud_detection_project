from django.db import models

class KPI(models.Model):
    nb_transactions = models.IntegerField()

    nb_cash_in = models.IntegerField()
    nb_cash_out = models.IntegerField()
    nb_transfer = models.IntegerField()
    nb_payment = models.IntegerField()
    nb_debit = models.IntegerField()
    avg_amount = models.FloatField()
    nb_fraud = models.IntegerField()
    nb_notfraud = models.IntegerField()

    def __init__(self, results):
        self.nb_transactions = results["nb_transactions"]
        self.nb_cash_in = results["nb_cash_in"]
        self.nb_cash_out = results["nb_cash_out"]
        self.nb_transfer = results["nb_transfer"]
        self.nb_payment = results["nb_payment"]
        self.nb_debit = results["nb_debit"]
        self.avg_amount = results["avg_amount"]
        self.nb_fraud = results["nb_fraud"]
        self.nb_notfraud = results["nb_notfraud"]
    def __str__(self):
        return f'Total transactions: {self.nb_transactions}\nTotal cash_in transactions: {self.nb_cash_in}\nTotal cash_out transactions: {self.nb_cash_out}\nTotal transfer transactions: {self.nb_transfer}\nTotal payment transactions: {self.nb_payment}\nTotal debit transactions: {self.nb_debit}\nTAverage amount of transactions: {self.avg_amount}\nNb of frauds: {self.nb_fraud}\nNb of non fraud: {self.nb_notfraud}\n'
