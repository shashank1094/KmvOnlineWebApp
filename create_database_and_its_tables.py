import MySQLdb
import logging

username = "root"
password = "root123"
database_name = "kmvonline"
host_name = "localhost"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('Tables Creation.log')  # create a file handler
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  # create a logging format
handler.setFormatter(formatter)
logger.addHandler(handler)  # add the handlers to the logger
logger.info('Starting to create database and tables inside it.')

try:
    db = MySQLdb.connect(host_name, username, password)
except MySQLdb.OperationalError:
    logger.error("Can't get connection to database.", exc_info=True)
    quit(1)


db.autocommit(False)
cur = db.cursor()  # Creating a Cursor object so that it executes all the queries

cur.execute("SELECT VERSION()")  # execute SQL query using execute() method.
data = cur.fetchone()  # Fetch a single row using fetchone() method.
logger.info("Version of Mysql : %s", data[0])

try:
    cur.execute("DROP DATABASE IF EXISTS " + database_name)
    cur.execute("CREATE DATABASE " + database_name)  # Create our database.
    logger.error("Database created.")
    # Create table as per requirement
    sql = """CREATE TABLE users (
                 id BIGINT NOT NULL PRIMARY KEY,              
                 name VARCHAR(40) NOT NULL,
                 designation VARCHAR(50),
                 username VARCHAR(25) NOT NULL,  
                 password VARCHAR(100) NOT NULL,
                 department INT NOT NULL)"""
    db.commit()
except MySQLdb.OperationalError:
    logger.error("Can't create table.", exc_info=True)
    db.rollback()


db.close()

logger.info('Finished.')
