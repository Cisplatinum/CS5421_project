from ipaddress import ip_address
from django.db import models


class Warehouse(models.Model):
    warehouse_id = models.AutoField(primary_key=True)
    warehouse_name = models.CharField(max_length=200)
    warehouse_street = models.CharField(max_length=200)
    warehouse_city = models.CharField(max_length=200)
    warehouse_country = models.CharField(max_length=200)

    # class Meta:

    #     unique_together = (("warehouse", "items"),)


class Items(models.Model):
    i_id = models.IntegerField(primary_key=True)
    i_im_id = models.CharField(max_length=50)
    i_name = models.CharField(max_length=50)
    i_price = models.DecimalField(max_digits=5, decimal_places=2)


class Stocks(models.Model):
    stocks_id = models.AutoField(primary_key=True)
    warehouse = models.ForeignKey(
        Warehouse, on_delete=models.CASCADE)
    items = models.ForeignKey(
        Items, on_delete=models.CASCADE)
    stocks_qty = models.IntegerField()


class Bigtable(models.Model):
    first_name = models.CharField(max_length=200)
    last_name = models.CharField(max_length=200)
    email = models.CharField(max_length=200)
    gender = models.CharField(max_length=30)
    ip_address = models.CharField(max_length=200)
    hometown_city = models.CharField(max_length=200)
    contact_number = models.CharField(max_length=200)
    ssn = models.CharField(max_length=200)
    credit_card = models.CharField(max_length=200)
    credit_card_type = models.CharField(max_length=200)
    job_title = models.CharField(max_length=200)
    university = models.CharField(max_length=200)
    linkedin_skills = models.CharField(max_length=200)
