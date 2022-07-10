import csv
import sqlite3

reader0 = csv.DictReader(open("data/shipping_data_0.csv"))
connection = sqlite3.connect("shipment_database.db")
cursor = connection.cursor()

product = {}        # a dictionary to record all the product and its corresponding product_id
quantity2 = {}      # a dictionary to record the quantity of a product in a specific shipment from shipping_data_1 and shipping_data_2
prodId = 0          #initial product Id
shipId = 0          #initial shipment Id

#Spreadsheet 0 is self contained and can simply be inserted into the database
for row in reader0:
    if not row["product"] in product:
        product[row["product"]] = prodId
        cursor.executescript("insert into product (id, name)  values (" + str(prodId) + ", \'" + row["product"] + "\');")
        prodId+=1
    cursor.executescript("insert into shipment (id, product_id, quantity, origin, destination)  values (" + str(shipId) + "," + str(product.get(row["product"])) + "," + row["product_quantity"]+", \'" + row["origin_warehouse"] + "\', \'"+ row["destination_store"]+"\');")
    shipId+=1

#first read from spreadsheet 1 to get the product and quantity, the reason we cannot direct write the sql insert command is that we don't know the quantity yet from our first read
reader1 = csv.DictReader(open("data/shipping_data_1.csv"))
for row in reader1:
    #still read in product and its corresponding product_id
    if not row["product"] in product:
        product[row["product"]] = prodId
        cursor.executescript("insert into product (id, name)  values (" + str(prodId) + ", \'" + row["product"] + "\');")
        prodId+=1
    #read in quantity of a product in a specific shipment from shipping_data_1
    binary_key = row["product"] + row["shipment_identifier"]
    if not binary_key in quantity2:
        quantity2[binary_key] = 1
    else:
        quantity2[binary_key] += 1
# now that we have the quantity, we are ready for writing sql command
# note that the unique key for every shipment record from shipping_data_1 to put in our database is (shipment_identifier,product)
# start from the top of shipping_data_1shipping_data_1
reader1 = csv.DictReader(open("data/shipping_data_1.csv"))
for row in reader1:
    binary_key = row["product"] + row["shipment_identifier"]
    # if haven't put in the database then excute
    if binary_key in quantity2:
        reader2 = csv.DictReader(open("data/shipping_data_2.csv"))
        #pull out its origin and destination using its shipment_identifier
        for row2 in reader2:
            if row2["shipment_identifier"] == row["shipment_identifier"]:
                break
        cursor.executescript("insert into shipment (id, product_id, quantity, origin, destination)  values (" + str(shipId) + "," + str(product.get(row["product"])) + "," + str(quantity2[binary_key])+", \'" + row2["origin_warehouse"] + "\', \'"+ row2["destination_store"]+"\');")
        shipId+=1
        #mark this (shipment_identifier,product) as already been put into the database
        quantity2.pop(binary_key)






