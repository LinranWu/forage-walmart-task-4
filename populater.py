import csv
import sqlite3

reader0 = csv.DictReader(open("data/shipping_data_0.csv"))
reader1 = csv.DictReader(open("data/shipping_data_1.csv"))
reader2 = csv.DictReader(open("data/shipping_data_2.csv"))

connection = sqlite3.connect("shipment_database.db")
cursor = connection.cursor()

product = {}
prodId = 0
shipId = 0
for row in reader0:
    if not row["product"] in product:
        product[row["product"]] = prodId
        cursor.executescript("insert into product (id, name)  values (" + str(prodId) + ", \'" + row["product"] + "\');")
        prodId+=1
    cursor.executescript("insert into shipment (id, product_id, quantity, origin, destination)  values (" + str(shipId) + "," + str(product.get(row["product"])) + "," + row["product_quantity"]+", \'" + row["origin_warehouse"] + "\', \'"+ row["destination_store"]+"\');")
    shipId+=1
for row in reader1:
    if not row["product"] in product:
        product[row["product"]] = prodId
        cursor.executescript("insert into product (id, name)  values (" + str(prodId) + ", \'" + row["product"] + "\');")
        prodId+=1







