from app import app, utils
import urllib.request
from flask import request
import datetime
import simplejson as json
from time import sleep

# A route to get candles data
# http://127.0.0.1:5000/api/v1/futures/candles?sec=SiH1&date=2021-03-13
@app.route('/api/v1/futures/candles', methods=['GET'])
def api_futures_candles():
    if not 'sec' in request.args:
        return "Error: No security code provided.", 404
    securityString = request.args['sec'].lower()
    if not 'date' in request.args:
        return "Error: No date provided.", 404
    requestDateString = request.args['date']
    try:
        requestDate = datetime.datetime.strptime(requestDateString, "%Y-%m-%d")

        if requestDate.date() > datetime.date.today():
            return "Can't see future: '" + requestDateString + "'", 200

        if requestDate.date().year < 2015:
            return "Year should be 2015 or more: '" + requestDateString + "'", 200
    except:
        return "Bad date format: '" + requestDateString + "'", 200

    query = "https://iss.moex.com/iss/securities/%s.json" % (securityString)
    moexData = json.loads(urllib.request.urlopen(query).read())
    rows = moexData["description"]["data"]

    if len(rows) == 0:
        return "Error: Bad security code.", 404

    db = utils.get_db()
    if not db.connected:
        return db.message, 200

    while not utils.working_day(requestDate):
        requestDate = requestDate - datetime.timedelta(days=1)
    requestDateString = requestDate.strftime("%Y-%m-%d")

    yearAwayDate = requestDate - datetime.timedelta(days=365)
    if yearAwayDate.date().year < 2015:
        yearAwayDate = datetime.date(2015, 1, 1)
    previousDate = requestDate - datetime.timedelta(days=1)
    nextDate = requestDate + datetime.timedelta(days=1)

    # check if table exists
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s);", \
        (securityString + "_candles", ))
    if rows[0][0] == False:
        if not db.execute("CREATE TABLE %s (" \
            "epoch bigint, " \
            "trade_date character varying(10), " \
            "time_begin character varying(20), " \
            "interval int, " \
            "open_price numeric(10, 4), " \
            "close_price numeric(10, 4), " \
            "low_price numeric(10, 4), " \
            "high_price numeric(10, 4), " \
            "volume integer);" % (securityString + "_candles") ):
            db.close()
            return "Can't create table '%s_candles'. %s" % (securityString + "_candles", db.error), 200
    else:
        # select candles for that date
        rows = db.select("SELECT * FROM %s WHERE interval=1 AND epoch>%d AND epoch<%d ORDER BY epoch;", \
            (securityString + "_candles", utils.epoch(requestDate), utils.epoch(nextDate)))

        if rows is not None and len(rows) != 0:

            # get data for 1' for previuos day
            # get data for 10'
            # get data for 1D
            # process it all


            db.close()
            return json.dumps(rows), 200

    # if DB has no data, get it from MOEX
    candles = [[], [], []]
    candles[0] = read_all_candles(securityString, previousDate.strftime("%Y-%m-%d"), requestDateString, 1)
    candles[1] = read_all_candles(securityString, previousDate.strftime("%Y-%m-%d"), requestDateString, 10)
    candles[2] = read_all_candles(securityString, yearAwayDate.strftime("%Y-%m-%d"), requestDateString, 24)

    # write data to DB
    insert_candles(securityString, 1, candles[0], db)
    insert_candles(securityString, 10, candles[1], db)
    insert_candles(securityString, 24, candles[2], db)



    # get data for 1' from candles?
    # get data for 10' from candles?
    # get data for 1D from candles?
    # process it all



    db.close()
    return json.dumps(rows), 200

def read_all_candles(securityString, startDate, tillDate, interval):
    index = 0
    count = 0
    candles = []
    while True:
        query = "https://iss.moex.com/iss/engines/futures/markets/forts/securities/%s/" \
            "candles.json?from=%s&till=%s&interval=%d&iss.meta=off" \
            "&start=%d" % (securityString, startDate, tillDate, interval, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        dataRead = moexData["candles"]["data"]
        candles.extend(dataRead)
        index = len(candles)
        count += 1
        if len(dataRead) == 0 or count > 40:
            break
        sleep(0.10)
    return candles

def insert_candles(securityString, interval, candles, db):
    query = "INSERT INTO %s(epoch, trade_date, time_begin, interval, open_price, close_price, " \
        "low_price, high_price, volume) VALUES " % securityString + "_candles"
    arguments = ','.join("(%d, '%s', '%s', %d, %.4f, %.4f, %.4f, %.4f, %d)" \
        % (utils.epoch_from_str(row[6]), row[6][-9], row[6], interval, row[0], row[1], \
            row[2], row[3], row[4]) for row in candles)
    if not db.execute(query + arguments + ";"):
        db.close()
        return "Can't write to table '%s_candles'. %s" % (securityString + "_candles", db.error), 200