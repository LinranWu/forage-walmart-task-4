import csv
import sqlite3

reader0 = csv.DictReader(open("data/shipping_data_0.csv"))
reader1 = csv.DictReader(open("data/shipping_data_1.csv"))
reader2 = csv.DictReader(open("data/shipping_data_2.csv"))

connection = sqlite3.connect("shipment_database.db")
cursor = connection.cursor()

product = {}
i = 0
for row in reader1:
    if not row["product"] in product:
        product[row["product"]] = i
        print("insert into product (id, name)  values (" + str(i) + ", \'" + row["product"] + "\');")
        cursor.executescript("insert into product (id, name)  values (" + str(i) + ", \'" + row["product"] + "\');")
        i+=1







