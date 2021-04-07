import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from urllib.parse import urlparse
import os
import sys
import holidays
import datetime

#url = urlparse(os.environ.get('DATABASE_URL'))
#db = "dbname=%s user=%s password=%s host=%s " % (url.path[1:], url.username, url.password, url.hostname)
#schema = "schema.sql"
class PostgresDB:
    def __init__(self):
        self.connection_string = "dbname=marketdb user=trader password=pq4trader host=localhost"
        self.connected = False

    def select(self, *args):
        if not self.connected:
            return
        try:
            query = self.cursor.mogrify(*args)
            self.cursor.execute(query)
        except Exception as err:
            self.error_message(err)
            print(self.error)
            self.connection.rollback()
            return
        return self.cursor.fetchall()

    def execute(self, query):
        if not self.connected:
            return False
        try:
            self.cursor.execute(query)
        except Exception as err:
            self.error_message(err)
            self.connection.rollback()
            return False
        self.connection.commit()
        return True

    def close(self):
        if self.connected:
            self.cursor.close()
            self.connection.close()
            self.connected = False
            self.message = "Connection to DB is closed"

    def error_message(self, err):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_num = traceback.tb_lineno   
        
        # print the connect() error
        self.error = "\npsycopg2 ERROR: %s on line number: %d\n" %(str(err), line_num)
        self.error += "psycopg2 error type: %s\n" % str(err_type)
        # print the pgcode and pgerror exceptions
        #self.error += "pgerror: %d\n" % err.pgerror
        #self.error += "pgcode: %d\n" % err.pgcode


def get_db():
    db = PostgresDB()
    try:
        db.connection = psycopg2.connect(db.connection_string)
        db.cursor = db.connection.cursor()
        db.connected = True
        db.message = "DB is connected"
    except OperationalError as err:
        db.error_message(err)
        db.message = "Can't connect to DB" + db.error
        
    return db

ru_holidays = holidays.RU()

def epoch(dt):
    return int(round(dt.timestamp() * 1000))

def epoch_from_str(dateString):
    return epoch(datetime.datetime.strptime(dateString, "%Y-%m-%d %H:%M:%S"))

def working_day(dt):
    dt_date = dt.date()
    if dt_date in ru_holidays:
        return False
    dt_weekday = dt_date.weekday()
    return dt_weekday != 5 and dt_weekday != 6
  