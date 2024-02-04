
# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2
# Connect to MySQL
# Connect to DB2 or PostgreSql
# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.
def get_last_rowid():
  dsn_hostname = '127.0.0.1'
 dsn_user='postgres'        # e.g. "abc12345"
 dsn_pwd ='MTM5MzMtbGFrc2ht'      # e.g. "7dBZ3wWt9XN6$o0J"
 dsn_port ="5432"                # e.g. "50000" 
 dsn_database ="postgres"           # i.e. "BLUDB"
# create connection
conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port)
#Crreate a cursor onject using cursor() method
cursor = conn.cursor()
# create table
SQL = """ select rowid from sales order by rowid desc limit 5  """
# Execute the SQL statement
cursor.execute(SQL)
print("got last row id")
	
last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)
   pass
  

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
def get_latest_records(rowid):
    connection = mysql.connector.connect(user='root', password='MTg0NDgtbmFnaWF0',host='127.0.0.1',database='sales')
    cursor = connection.cursor
    cursor.execute("SELECT * FROM sales WHERE rowid > %s", (rowid,))  # Fetch records with rowid > last_row_id
    new_records = cursor.fetchall()
    return new_records  # Return the list of new records
    print("New rows on staging datawarehouse = ", len(new_records))
    pass
# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
 dsn_hostname = '127.0.0.1'
 dsn_user='postgres'        # e.g. "abc12345"
 dsn_pwd ='MTM5MzMtbGFrc2ht'      # e.g. "7dBZ3wWt9XN6$o0J"
 dsn_port ="5432"                # e.g. "50000" 
 dsn_database ="postgres"           # i.e. "BLUDB"
# create connection
conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port)
   cursor = conn.cursor()
    for row in records:  # Iterate through the actual new_records list
        cursor.execute("INSERT INTO sales_data(rowid, product, category) VALUES(%s, %s, %s)", row)
        conn.commit()
insert_records([(5,'Mobile','Electronics'),(6,'Mobile','Electronics')])
print("New rows inserted into production datawarehouse = ", len(new_records))
# disconnect from mysql warehouse
connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
conn.close()
# End of program