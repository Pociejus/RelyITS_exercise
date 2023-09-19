import mysql.connector

'''
Database creation in MySQL, Table creation in database named XML 
'''

db_connection = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin"
)

db_cursor = db_connection.cursor()
create_database_query = "CREATE DATABASE IF NOT EXISTS XML"
db_cursor.execute(create_database_query)

# Table creation in XML database
db_cursor.execute("USE XML")

create_transaction_table_query = """
CREATE TABLE IF NOT EXISTS Transaction (
    TransactionID VARCHAR(255) PRIMARY KEY,
    BeginDateTime DATETIME,
    EndDateTime DATETIME
)
"""
db_cursor.execute(create_transaction_table_query)

# Creating the 'RetailTransactions' table with a foreign key constraint on TransactionID
create_table_query = """
CREATE TABLE IF NOT EXISTS RetailTransactions (
    Date DATETIME,
    StoreID INT,
    TotalItems DECIMAL(10, 2),
    TotalAmount DECIMAL(10, 2),
    TotalReceipts DECIMAL(10, 2),
    TransactionID VARCHAR(255),
    FOREIGN KEY (TransactionID) REFERENCES Transaction(TransactionID)
)
"""
db_cursor.execute(create_table_query)

db_cursor.close()
db_connection.close()
