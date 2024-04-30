
import random
import os
import csv
from datetime import datetime, timedelta

customer_ids = list(range(1, 21))
store_ids = list(range(100, 116))

product_data = {
    "quaker oats": 212,
    "sugar": 50,
    "maida": 20,
    "besan": 52,
    "refined oil": 110,
    "clinic plus": 1.5,
    "dantkanti": 100,
    "nutrella": 40
}

sales_persons = {
    100: [1, 2, 3],
    101: [4, 5, 6],
    102: [7, 8, 9],
    103: [10, 11, 12],
    104: [13, 14, 15],
    105: [16, 17, 18],
    106: [19, 2, 3],
    107: [4, 5, 6],
    108: [7, 8, 9],
    109: [1, 2, 3],
    110: [4, 5, 6],
    111: [7, 8, 9],
    112: [1, 2, 3],
    113: [4, 5, 6],
    114: [7, 8, 9],
    115: [1, 2, 3]
}

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 3, 30)

file_location = "/home/cbnits-87/project_data/data_engineering/spark_data"
csv_file_path = os.path.join(file_location, "sales_data.csv")

with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                    "total_cost"])

    for _ in range(500):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        product_name = random.choice(list(product_data.keys()))
        sales_date = start_date + timedelta(days=random.randint(0, (end_date-start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 20)
        price = product_data[product_name]
        total_cost = quantity * price

        csvwriter.writerow([customer_id, store_id, product_name, sales_date, sales_person_id, price,
                            quantity, total_cost])

print("CSV data written successfully")
