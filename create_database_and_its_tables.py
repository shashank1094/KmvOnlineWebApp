import MySQLdb, logging, getpass, warnings

# Uncomment next to line to take user's detail for mysql from keyboard.
# username = input("User : \n")
# password = getpass.getpass("Password : \n")
username = "root"
password = "root123"
database_name = "kmvonline"
host_name = "localhost"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('Tables Creation.log')  # create a file handler
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(lineno)d  - %(name)s - %(levelname)s - %(message)s')  # create a logging format
handler.setFormatter(formatter)
logger.addHandler(handler)  # add the handlers to the logger
logger.info('Starting to create database and tables inside it.')

try:
    db = MySQLdb.connect(host_name, username, password)
except MySQLdb.OperationalError:
    logger.error("Unable to get connection to database.", exc_info=True)
    print("Error while connecting to database. See >> Tables Creation.log << for more details.")
    quit(1)

db.autocommit(False)
cur = db.cursor()  # Creating a Cursor object so that it executes all the queries

cur.execute("SELECT VERSION()")  # execute SQL query using execute() method.
data = cur.fetchone()  # Fetch a single row using fetchone() method.
logger.info("Version of Mysql : %s", data[0])

try:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cur.execute("DROP DATABASE IF EXISTS " + database_name)
    # If Warnings are not ignored we get this message when kmvonline is not present and we try to drop it
    # create_database_and_its_tables.py:<line No>: Warning: (1008, "Can't drop database 'kmvonline'; database doesn't exist")
    # cur.execute("DROP DATABASE IF EXISTS " + database_name)

    cur.execute("CREATE DATABASE " + database_name)  # Create our database.
    logger.info("Database created.")
    # Create tables as per requirement
    sql = """CREATE TABLE """ + database_name + """.users (
                 id BIGINT NOT NULL PRIMARY KEY,              
                 name VARCHAR(40) NOT NULL,
                 designation VARCHAR(50),
                 username VARCHAR(25) NOT NULL,  
                 password VARCHAR(100) NOT NULL,
                 department INT NOT NULL)"""
    cur.execute(sql)
    logger.info("users table created.")
    sql = """CREATE TABLE """ + database_name + """.departments (
                 id BIGINT NOT NULL PRIMARY KEY,              
                 dname VARCHAR(40) NOT NULL)"""
    cur.execute(sql)
    logger.info("departments table created.")
    sql = """CREATE TABLE """ + database_name + """.notifications (
                 id BIGINT NOT NULL PRIMARY KEY,              
                 title VARCHAR(1000) NOT NULL,
                 description VARCHAR(5000) NOT NULL,
                 owner BIGINT NOT NULL,  
                 date_issued DATETIME NOT NULL)"""
    cur.execute(sql)
    logger.info("notifications table created.")

    db.commit()
except MySQLdb.OperationalError:
    logger.error("Unable to create table.", exc_info=True)
    print("Error while creating table(s). See >> Tables Creation.log << for more details.")
    db.rollback()
    exit(1)

db.close()

logger.info('Finished.')
print("Database and Tables created successfully.\n")
