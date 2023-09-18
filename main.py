import os
import xml.etree.ElementTree as ET
import mysql.connector
import csv

# Set up database connection
db_connection = mysql.connector.connect(
    host="host",
    user="admin",
    password="admin",
    database="XML_Info"
)
db_cursor = db_connection.cursor()

# Directory where XML files are deposited
xml_directory = 'python.interview-exercise/receipts'

# Aggregated data storage (e.g., a list or dictionary)
aggregated_data = []

# Iterate through XML files in the directory
for filename in os.listdir(xml_directory):
    if filename.endswith(".xml"):
        xml_file_path = os.path.join(xml_directory, filename)

        # Parse the XML file and extract data
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        # Extract and store data in the database
        # (Assuming you have already created the database tables)
        # Perform the required database insertions here

        # Extract and aggregate data (example)
        total_amount = 0
        for transaction in root.findall('.//RetailTransaction'):
            amount = float(transaction.find('.//RetailTransaction/Total[@TotalType="TransactionNetAmount"]').text)
            total_amount += amount

        aggregated_data.append({'Filename': filename, 'TotalAmount': total_amount})

# Generate CSV file with aggregated data
csv_file_path = 'aggregated_data.csv'
with open(csv_file_path, 'w', newline='') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=['Filename', 'TotalAmount'])
    csv_writer.writeheader()
    csv_writer.writerows(aggregated_data)

# Close the database connection
db_cursor.close()
db_connection.close()

print(f"CSV file '{csv_file_path}' generated.")

