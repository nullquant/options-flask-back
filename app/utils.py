from app import app
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from urllib.parse import urlparse
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
            self.connection.rollback()
            app.logger.info(self.error)
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
            app.logger.info(self.error)
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

def epoch_from_str(dateString, format_string="%Y-%m-%d %H:%M:%S"):
    if len(dateString) == 0:
        return dateString
    return int(round((datetime.datetime.strptime(dateString, format_string)).timestamp() * 1000))

def working_day(dt):
    dt_date = dt.date()
    if dt_date in ru_holidays:
        return False
    dt_weekday = dt_date.weekday()
    return dt_weekday != 5 and dt_weekday != 6

def get_option_month(month):
    return {
        'A': 1,
        'B': 2,
        'C': 3,
        'D': 4,
        'E': 5,
        'F': 6,
        'G': 7,
        'H': 8,
        'I': 9,
        'J': 10,
        'K': 11,
        'L': 12,
        'M': 1,
        'N': 2,
        'O': 3,
        'P': 4,
        'Q': 5,
        'R': 6,
        'S': 7,
        'T': 8,
        'U': 9,
        'V': 10,
        'W': 11,
        'X': 12
    }.get(month, 0)

def get_call_month(month):
    return {
        1: 'A',
        2: 'B',
        3: 'C',
        4: 'D',
        5: 'E',
        6: 'F',
        7: 'G',
        8: 'H',
        9: 'I',
        10: 'J',
        11: 'K',
        12: 'L'
    }.get(month, 'A')

def get_put_month(month):
    return {
        1: 'M',
        2: 'N',
        3: 'O',
        4: 'P',
        5: 'Q',
        6: 'R',
        7: 'S',
        8: 'T',
        9: 'U',
        10: 'V',
        11: 'W',
        12: 'X'
    }.get(month, 'M')

def futures_code(name):
    month = {
        1: 'F',
        2: 'G',
        3: 'H',
        4: 'J',
        5: 'K',
        6: 'M',
        7: 'N',
        8: 'Q',
        9: 'U',
        10: 'V',
        11: 'X',
        12: 'Z',
    }.get(int(name[name.index('-')+1:name.index('.')]))
    return name[:2] + month + name[name.index('M')-1]

def futures_name(name):
    month = {
        'F': '-1.',
        'G': '-2.',
        'H': '-3.',
        'J': '-4.',
        'K': '-5.',
        'M': '-6.',
        'N': '-7.',
        'Q': '-8.',
        'U': '-9.',
        'V': '-10.',
        'X': '-11.',
        'Z': '-12.'
    }.get(name[2], '.')
    return name[:2] + month + '2' + name[-1]

def strike_delta(name):
    return { "si": 250, "ri": 2500, "br": 1 }.get(name[:2].lower())
