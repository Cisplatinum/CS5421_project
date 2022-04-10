from django.shortcuts import render
from project.models import Warehouse, Bigtable, Items, Stocks
from itertools import islice
from django.http import HttpResponse
import json
import time
import os


def index(request):
    ################with foreign key################
    # with open(os.path.dirname(os.path.realpath("./warehouse.json")) + "/warehouse/warehouse.json", "r") as f:
    #     warehouse = json.loads(f.read())
    # num_rows = len(warehouse)
    # # Experiment 1: Single insert
    # start = time.time()

    # for i in range(num_rows):
    #     wh = warehouse[i]
    #     q = Warehouse(
    #         warehouse_name=wh['w_name'],
    #         warehouse_street=wh['w_street'],
    #         warehouse_city=wh['w_city'],
    #         warehouse_country=wh['w_country']
    #     )
    #     q.save()

    # end = time.time()
    # print(f'Django SINGLE INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # return HttpResponse(f'Django SINGLE INSERT Rows/sec:{num_rows/(end-start): .2f}')

    # Experiment 2: Bulk insert
    # start = time.time()

    # BATCH_SIZE = 50
    # objs = (Warehouse(
    #     warehouse_name=warehouse[i]['w_name'],
    #     warehouse_street=warehouse[i]['w_street'],
    #     warehouse_city=warehouse[i]['w_city'],
    #     warehouse_country=warehouse[i]['w_country']
    # ) for i in range(num_rows))
    # while True:
    #     batch = list(islice(objs, BATCH_SIZE))
    #     if not batch:
    #         break
    #     Warehouse.objects.bulk_create(batch, BATCH_SIZE)
    # end = time.time()
    # print(
    #     f'Django BATCH ({BATCH_SIZE}) INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # return HttpResponse(f'Django BATCH ({BATCH_SIZE}) INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # Experiment 3a: Single select by city
    # count = 0
    # cities = [
    #     'Stockholm',
    #     'Oslo',
    #     'Buenavista',
    #     'San Antonio',
    #     'Washington'
    # ]

    # start = time.time()

    # for _ in range(100):
    #     for city in cities:
    #         res = list(Warehouse.objects.all().filter(warehouse_city=city))
    #         count += len(res)

    # end = time.time()
    # print(f'Django Single SELECT by city Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django Single SELECT by city Rows/sec: {count/(end-start): .2f}')

    # Experiment 3b: Single select by city and street
    # count = 0
    # addresses = [
    #     ('Nanjing', 'Starling'),
    #     ('Wudui', 'American'),
    #     ('Xiangyang', 'Bunker Hill'),
    #     ('Juntas', 'Bluestem'),
    #     ('Sinfra', 'Jenna')
    # ]
    # start = time.time()

    # for _ in range(100):
    #     for city, street in addresses:
    #         res = list(Warehouse.objects.all().filter(
    #             warehouse_city=city, warehouse_street=street))
    #         count += len(res)

    # end = time.time()
    # print(
    # f'Django Single SELECT by city and street Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django Single SELECT by city and street Rows/sec: {count/(end-start): .2f}')
    # Experiment 3c: Single select by city limit 20
    # limit = 20
    # count = 0
    # cities = [
    #     'Stockholm',
    #     'Oslo',
    #     'Buenavista',
    #     'San Antonio',
    #     'Washington'
    # ]

    # start = time.time()

    # for _ in range(100):
    #     for city in cities:
    #         res = list(Warehouse.objects.all().filter(
    #             warehouse_city=city)[:20])
    #         count += len(res)

    # end = time.time()
    # print(
    #     f'Django Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')

    # experienment: select with w_city and item

    # count = 0
    # city_iname = [
    #     ('Stockholm', 'Pedi-Dri'),
    #     ('Hesheng', 'Glyburide'),
    #     ('Ciekek', 'Hydrocortisone'),
    #     ('Oslo', 'ACNE SOLUTIONS'),
    #     ('Washington', 'Doxycycline Hyclate')
    # ]

    # start = time.time()

    # for _ in range(100):
    #     for city, i_name in city_iname:
    #         q = Stocks.objects.filter(items__i_name=i_name)
    #         c = q.filter(warehouse__warehouse_city=city+" Update")
    #         count += len(c)
    # end = time.time()
    # print(
    #     f'Django Single SELECT stock join warehouse by city and i_name Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django SINGLE INSERT Rows/sec:{3}')

    # experienment: select with w_city and item
    # count = 0
    # cities = [
    #     'Santa Cruz',
    #     'Washington',
    #     'Bibrka',
    #     'Haninge',
    #     'Inta'
    # ]

    # start = time.time()

    # for _ in range(100):
    #     for city in cities:
    #         c = Stocks.objects.filter(warehouse__warehouse_city=city+" Update")
    #         count += len(c)

    # end = time.time()
    # print(
    #     f'django Single SELECT stock join warehouse by city Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django SINGLE INSERT Rows/sec:{3}')

    # Update - update the whole object

    # objs = list(Warehouse.objects.all())
    # count = len(objs)

    # start = time.time()

    # for obj in objs:
    #     obj. warehouse_name = f"{obj.warehouse_name} Update"
    #     obj. warehouse_street = f"{obj.warehouse_street} Update"
    #     obj. warehouse_city = f"{obj.warehouse_city} Update"
    #     obj. warehouse_country = f"{obj.warehouse_country} Update"
    #     obj.save()

    # end = time.time()
    # print(f"Django Update whole table Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Update whole table Rows/sec: {count / (end - start): .2f}")

    # update single column of object

    # objs = list(Warehouse.objects.all())
    # count = len(objs)
    # start = time.time()
    # for obj in objs:
    #     obj.warehouse_country = f"{obj.warehouse_country} Update2"
    #     obj.save()
    # end = time.time()
    # print(f"Django Update partial Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Update partial Rows/sec: {count / (end - start): .2f}")

    # delete

    # objs = list(Warehouse.objects.all())
    # count = len(objs)

    # start = time.time()

    # for obj in objs:
    #     obj.delete()

    # end = time.time()
    # print(f"Django Delete Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Delete Rows/sec: {count / (end - start): .2f}")

    ################without foreign key #################
    # with open(os.path.dirname(os.path.realpath("./warehouse.json")) + "/warehouse/warehouse.json", "r") as f:
    #     warehouse = json.loads(f.read())
    # num_rows = len(warehouse)
    # # Experiment 1: Single insert
    # start = time.time()

    # for i in range(num_rows):
    #     wh = warehouse[i]
    #     q = Warehouse(
    #         warehouse_name=wh['w_name'],
    #         warehouse_street=wh['w_street'],
    #         warehouse_city=wh['w_city'],
    #         warehouse_country=wh['w_country']
    #     )
    #     q.save()

    # end = time.time()
    # print(f'Django SINGLE INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # return HttpResponse(f'Django SINGLE INSERT Rows/sec:{num_rows/(end-start): .2f}')

    # Experiment 2: Bulk insert
    # start = time.time()

    # BATCH_SIZE = 50
    # objs = (Warehouse(
    #     warehouse_name=warehouse[i]['w_name'],
    #     warehouse_street=warehouse[i]['w_street'],
    #     warehouse_city=warehouse[i]['w_city'],
    #     warehouse_country=warehouse[i]['w_country']
    # ) for i in range(num_rows))
    # while True:
    #     batch = list(islice(objs, BATCH_SIZE))
    #     if not batch:
    #         break
    #     Warehouse.objects.bulk_create(batch, BATCH_SIZE)
    # end = time.time()
    # print(
    #     f'Django BATCH ({BATCH_SIZE}) INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # return HttpResponse(f'Django BATCH ({BATCH_SIZE}) INSERT Rows/sec:{num_rows/(end-start): .2f}')

    # Experiment 3a: Single select by city
    # count = 0
    # cities = [
    #     'Stockholm',
    #     'Oslo',
    #     'Buenavista',
    #     'San Antonio',
    #     'Washington'
    # ]

    # start = time.time()

    # for _ in range(100):
    #     for city in cities:
    #         res = list(Warehouse.objects.all().filter(warehouse_city=city))
    #         count += len(res)

    # end = time.time()
    # print(f'Django Single SELECT by city Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django Single SELECT by city Rows/sec: {count/(end-start): .2f}')

    # Experiment 3b: Single select by city and street
    # count = 0
    # addresses = [
    #     ('Nanjing', 'Starling'),
    #     ('Wudui', 'American'),
    #     ('Xiangyang', 'Bunker Hill'),
    #     ('Juntas', 'Bluestem'),
    #     ('Sinfra', 'Jenna')
    # ]
    # start = time.time()

    # for _ in range(100):
    #     for city, street in addresses:
    #         res = list(Warehouse.objects.all().filter(
    #             warehouse_city=city, warehouse_street=street))
    #         count += len(res)

    # end = time.time()
    # print(
    #     f'Django Single SELECT by city and street Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django Single SELECT by city and street Rows/sec: {count/(end-start): .2f}')
    # Experiment 3c: Single select by city limit 20
    # limit = 20
    # count = 0
    # cities = [
    #     'Stockholm',
    #     'Oslo',
    #     'Buenavista',
    #     'San Antonio',
    #     'Washington'
    # ]

    # start = time.time()

    # for _ in range(100):
    #     for city in cities:
    #         # res = list(Warehouse.objects.all().filter(
    #         #     warehouse_city=city, pk__lte=limit))
    #         res = list(Warehouse.objects.all().filter(
    #             warehouse_city=city)[:20])
    #         count += len(res)

    # end = time.time()
    # print(
    #     f'Django Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')
    # return HttpResponse(f'Django Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')

    # Update - update the whole object

    # objs = list(Warehouse.objects.all())
    # count = len(objs)

    # start = time.time()

    # for obj in objs:
    #     obj. warehouse_name = f"{obj.warehouse_name} Update"
    #     obj. warehouse_street = f"{obj.warehouse_street} Update"
    #     obj. warehouse_city = f"{obj.warehouse_city} Update"
    #     obj. warehouse_country = f"{obj.warehouse_country} Update"
    #     obj.save()

    # end = time.time()
    # print(f"Django Update whole table Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Update whole table Rows/sec: {count / (end - start): .2f}")

    # update single column of object

    # objs = list(Warehouse.objects.all())
    # count = len(objs)
    # start = time.time()
    # for obj in objs:
    #     obj.warehouse_country = f"{obj.warehouse_country} Update2"
    #     obj.save()
    # end = time.time()
    # print(f"Django Update partial Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Update partial Rows/sec: {count / (end - start): .2f}")

    # delete

    # objs = list(Warehouse.objects.all())
    # count = len(objs)

    # start = time.time()

    # for obj in objs:
    #     obj.delete()

    # end = time.time()
    # print(f"Django Delete Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Delete Rows/sec: {count / (end - start): .2f}")

    ################BIGTABLE################
    with open("/Users/yitianliu/Documents/master/sem2/cs5421/project/bigtable.json", "r") as f:
        bigtable = json.loads(f.read())
    num_rows = len(bigtable)

    # Experiment 1: Single insert
    start = time.time()

    for i in range(num_rows):
        bt = bigtable[i]
        q = Bigtable(
            first_name=bt['first_name'],
            last_name=bt['last_name'],
            email=bt['email'],
            gender=bt['gender'],
            ip_address=bt['ip_address'],
            hometown_city=bt['hometown_city'],
            contact_number=bt['contact_number'],
            ssn=bt['ssn'],
            credit_card=bt['credit_card'],
            credit_card_type=bt['credit_card_type'],
            job_title=bt['job_title'],
            university=bt['university'],
            linkedin_skills=bt['Linkedin_skills'],
        )
        q.save()

    end = time.time()
    # print(
    #     f'Django big table SINGLE INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # return HttpResponse(f'Django big table SINGLE INSERT Rows/sec:{num_rows/(end-start): .2f}')

# Experiment 2: Bulk insert
    # start = time.time()

    # BATCH_SIZE = 50
    # objs = (Bigtable(
    #         first_name=bigtable[i]['first_name'],
    #         last_name=bigtable[i]['last_name'],
    #         email=bigtable[i]['email'],
    #         gender=bigtable[i]['gender'],
    #         ip_address=bigtable[i]['ip_address'],
    #         hometown_city=bigtable[i]['hometown_city'],
    #         contact_number=bigtable[i]['contact_number'],
    #         ssn=bigtable[i]['ssn'],
    #         credit_card=bigtable[i]['credit_card'],
    #         credit_card_type=bigtable[i]['credit_card_type'],
    #         job_title=bigtable[i]['job_title'],
    #         university=bigtable[i]['university'],
    #         linkedin_skills=bigtable[i]['Linkedin_skills'],
    #         ) for i in range(num_rows))
    # while True:
    #     batch = list(islice(objs, BATCH_SIZE))
    #     if not batch:
    #         break
    #     Bigtable.objects.bulk_create(batch, BATCH_SIZE)
    # end = time.time()
    # print(
    #     f'Django BATCH ({BATCH_SIZE}) INSERT Rows/sec:{num_rows/(end-start): .2f}')
    # return HttpResponse(f'Django big table BATCH ({BATCH_SIZE}) INSERT Rows/sec:{num_rows/(end-start): .2f}')

# Experiment 3a: Single select by city
    count = 0
    cities = [
        'Vinces',
        'Shanxi',
        'Jianghong',
        'Noailles',
        'Gucheng'
    ]

    start = time.time()

    for _ in range(100):
        for city in cities:
            res = list(Bigtable.objects.all().filter(hometown_city=city))
            count += len(res)

    end = time.time()
    print(f'Django Single SELECT by city Rows/sec: {count/(end-start): .2f}')
# return HttpResponse(f'Django big table Single SELECT by gender Rows/sec: {count/(end-start): .2f}')

# Experiment 3b: Single select by city and gender
    # count = 0
    # genderncity = [
    #     ('Female', 'Vinces'),
    #     ('Male', 'Shanxi'),
    #     ('Non-binary', 'Jianghong'),
    #     ('Agender', 'Noailles'),
    #     ('Polygender', 'Gucheng')
    # ]
    # start = time.time()

    # for _ in range(100):
    #     for gender, city in genderncity:
    #         res = list(Bigtable.objects.all().filter(
    #             gender=gender, hometown_city=city))
    #         count += len(res)

    # end = time.time()
    # print(
    #     f'Django Single SELECT by gender and city Rows/sec: {count/(end-start): .2f}')
# return HttpResponse(f'Django big table Single SELECT by gender and city Rows/sec: {count/(end-start): .2f}')
# Experiment 3c: Single select by city limit 20
    limit = 20
    count = 0
    cities = [
        'Vinces',
        'Shanxi',
        'Jianghong',
        'Noailles',
        'Gucheng'
    ]

    start = time.time()

    for _ in range(100):
        for city in cities:
            res = list(Bigtable.objects.all().filter(
                hometown_city=city)[:limit])
            count += len(res)

    end = time.time()
    print(
        f'Django bigtable Single SELECT by gender limit 20 Rows/sec: {count/(end-start): .2f}')
    return HttpResponse(f'Django bigtable Single SELECT by city limit 20 Rows/sec: {count/(end-start): .2f}')

    # Update - update the whole object

    # objs = list(Bigtable.objects.all())
    # count = len(objs)

    # start = time.time()

    # for obj in objs:
    #     obj.first_name = f"{obj.first_name} Update"
    #     obj.last_name = f"{obj.last_name} Update"
    #     obj.email = f"{obj.email} Update"
    #     obj.gender = f"{obj.gender} Update"
    #     obj.ip_address = f"{obj.ip_address} Update"
    #     obj.hometown_city = f"{obj.hometown_city} Update"
    #     obj.contact_number = f"{obj.contact_number} Update"
    #     obj.ssn = f"{obj.ssn} Update"
    #     obj.credit_card = f"{obj.credit_card} Update"
    #     obj.credit_card_type = f"{obj.credit_card_type} Update"
    #     obj.job_title = f"{obj.job_title} Update"
    #     obj.university = f"{obj.university} Update"
    #     obj.linkedin_skills = f"{obj.linkedin_skills} Update"
    #     obj.save()

    # end = time.time()
    # print(
    #     f"Django Big table Update whole table Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Big table Update whole table Rows/sec: {count / (end - start): .2f}")

    # update single column of object

    # objs = list(Bigtable.objects.all())
    # count = len(objs)
    # start = time.time()
    # for obj in objs:
    #     obj.hometown_city = f"{obj.hometown_city} Update 2"
    #     obj.save()
    # end = time.time()
    # print(
    #     f"Django Big table Update partial Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django Big table Update partial Rows/sec: {count / (end - start): .2f}")

    # delete

    # objs = list(Bigtable.objects.all())
    # count = len(objs)

    # start = time.time()

    # for obj in objs:
    #     obj.delete()

    # end = time.time()
    # print(f"Django big table Delete Rows/sec: {count / (end - start): .2f}")
    # return HttpResponse(f"Django big table Delete Rows/sec: {count / (end - start): .2f}")
