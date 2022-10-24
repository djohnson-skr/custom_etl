from datetime import datetime, timedelta
from pprint import pprint
import csv
from time import strftime
from f_name import f_name
from l_name import l_name
from shoes import shoes
import random
from faker import Faker
from write_to_s3 import write_to_s3


header = ['first_name', 'last_name', 'shoe', 'price', 'credit_card', 'purchase_date']
total_rows = 500
time_now = datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
fake = Faker()
start_time = datetime.now() - timedelta(hours=0, minutes=1)

# for write_to_s3()
file_dir = '/Users/ArminHammer/Documents/etl/csv_data/'
bucket_name = 'snowpipe-armin'

def get_first_name():
  return random.choice(f_name) 

def get_last_name():
  return random.choice(l_name)

def get_shoe():
  return random.choice(shoes)

def get_price():
  return random.randrange(40, 200)

def get_credit_card():
  return fake.credit_card_number(card_type='visa16')

def get_purchase_date():
  rand = random.randrange(0,59)
  date = start_time + timedelta(hours=0, minutes=0, seconds=rand)
  date = date.replace(microsecond=0)
  return date

def gather_data():
  data = [get_first_name(), get_last_name(), get_shoe(), get_price(), get_credit_card(), get_purchase_date()]
  return data

with open('/Users/ArminHammer/Documents/etl/csv_data/purchases' + '_' + time_now + '.csv', 'w') as f:
  writer = csv.writer(f)
  writer.writerow(header)
  for i in range(0, total_rows):
    writer.writerow(gather_data())

upload = write_to_s3(file_dir, bucket_name)