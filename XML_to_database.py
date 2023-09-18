import os
import mysql.connector
import xml.etree.ElementTree as ET
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
'''
Continuing with created database. Uploading data to a table
'''

# Connecting to a server
db_connection = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin",
    database="XML"
)

xml_directory = r'python.interview-exercise\receipts'

# SQL query for upload to a table
insert_query = """
INSERT INTO RetailTransactions (Date, StoreID, TotalItems, TotalAmount, TotalReceipts)
VALUES (%s, %s, %s, %s, %s)
"""

# Creating database cursor
db_cursor = db_connection.cursor()

db_cursor.execute("SELECT COUNT(*) FROM RetailTransactions")
existing_records_before_insert = db_cursor.fetchone()[0]

for filename in os.listdir(xml_directory):
    if filename.endswith(".xml"):

        xml_file_path = os.path.join(xml_directory, filename)

        # Downloading XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        data_to_insert = []

        # Get items and elements
        namespace = {'ns': 'http://www.nrf-arts.org/IXRetail/namespace/'}
        id_item = root.find('.//ns:BusinessUnit/ns:UnitID', namespaces=namespace)
        unit_id = id_item.text

        quantity_item = root.find('.//ns:RetailTransaction/ns:LineItem/ns:Sale/ns:Quantity', namespaces=namespace)
        quantity = float(quantity_item.text)

        try:
            total_amount_item = root.find('.//ns:RetailTransaction/ns:Total[@TotalType="TransactionNetAmount"]',
                                          namespaces=namespace)
            total_amount = float(total_amount_item.text)

        except AttributeError:
            total_element = root.find('.//ns:RetailTransaction/ns:Total[@TotalType="VRExt:TransactionVatRateAmount"]',
                                      namespaces=namespace)
            vat_excl_amount = total_element.get('{http://schemas.vismaretail.com/poslog/}VATExclAmount')
            total_amount = float(vat_excl_amount)

        '''
        Try to take from RetailTransaction\Total[TotalType=”TransactionNetAmount”]
        in case the field is absent, take from RetailTransaction\Total[@TotalType="VRExt:TransactionVatRateAmount"]"
        because the numbers are equal to the first ones
        '''

        total_receipts = quantity * total_amount

        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        data_to_insert.append((current_date, unit_id, quantity, total_amount, total_receipts))

        logging.info(data_to_insert)
        # Adding data to a table
        db_cursor.executemany(insert_query, data_to_insert)

db_connection.commit()

db_cursor.execute("SELECT COUNT(*) FROM RetailTransactions")
existing_records_after_insert = db_cursor.fetchone()[0]

db_cursor.close()

db_connection.close()

logging.info(f'Eilučių lentelėje prieš įkėlimą: {existing_records_before_insert}')
logging.info(f'Eilučių lentelėje po įkėlimo: {existing_records_after_insert}')
