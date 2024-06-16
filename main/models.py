from django.db import models

# Create your models here.


class Pairs(models.Model):
    Pair_Name = models.CharField('Pair Name', max_length=10)
    Pair_quote = models.CharField('Pair Quote', max_length=10, default='')
    Pair_base = models.CharField('Pair Base', max_length=10, default='')
    Spend_amount = models.FloatField('Spend amount', max_length=50)
    active = models.BooleanField('Active', default=True)

    def __str__(self):
        return self.Pair_Name

    def get_absolute_url(self):
        return f'/pairs'


class Keys(models.Model):
    API_KEY = models.CharField('API_KEY', max_length=200)
    API_SECRET = models.CharField('API_SECRET', max_length=200)


class BotPath(models.Model):
    Bot_Path_Body = models.CharField('Bot_Path_Body', max_length=300)
    Bot_Path_Gen = models.CharField('Bot_Path_Gen', max_length=300)
