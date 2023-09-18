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


create_table_query = """
CREATE TABLE IF NOT EXISTS RetailTransactions (
    Date DATETIME,
    StoreID INT,
    TotalItems DECIMAL(10, 2),
    TotalAmount DECIMAL(10, 2),
    TotalReceipts DECIMAL(10, 2)
)
"""
db_cursor.execute(create_table_query)

# Close connection
db_cursor.close()
db_connection.close()