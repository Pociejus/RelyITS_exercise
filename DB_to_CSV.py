import mysql.connector
import csv
from datetime import datetime

'''
Taking all data from table and putting it to a CSV file for a client
'''

db_connection = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="XML"
)

db_cursor = db_connection.cursor()

# SQL query to get data
query = """
SELECT StoreID, SUM(TotalItems) AS TotalItems, SUM(TotalAmount) AS TotalAmount, SUM(TotalReceipts) AS TotalReceipts
FROM RetailTransactions
GROUP BY StoreID
"""

db_cursor.execute(query)
date = datetime.now().strftime("%Y-%m-%d")
# all data to scv
with open("client.csv", "w", newline="") as csvfile:
    csv_writer = csv.writer(csvfile)

    csv_writer.writerow(["Date", "StoreID", "TotalItems", "TotalAmount", "TotalReceipts"])

    for row in db_cursor.fetchall():
        store_id, total_items, total_amount, total_receipts = row
        csv_writer.writerow([date, store_id, total_items, total_amount, total_receipts])

db_cursor.close()
db_connection.close()

print("CSV failas sukurtas sėkmingai.")
