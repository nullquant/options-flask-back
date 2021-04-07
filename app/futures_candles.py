from app import app, utils, ema
import urllib.request
from flask import request
import datetime
import simplejson as json
from time import sleep
import numpy as np
import time

# A route to get candles data
# http://127.0.0.1:5000/api/v1/futures/candles?sec=SiH1&date=2021-03-13
@app.route('/api/v1/futures/candles', methods=['GET'])
def api_futures_candles():
    if not 'sec' in request.args:
        return "Error: No security code provided.", 400
    securityString = request.args['sec'].lower()
    if not 'date' in request.args:
        return "Error: No date provided.", 400
    requestDateString = request.args['date']
    try:
        requestDate = datetime.datetime.strptime(requestDateString, "%Y-%m-%d")

        if requestDate.date() > datetime.date.today():
            return "Can't see future: '" + requestDateString + "'", 400

        if requestDate.date().year < 2015:
            return "Year should be 2015 or more: '" + requestDateString + "'", 400
    except:
        return "Bad date format: '" + requestDateString + "'", 400

    query = "https://iss.moex.com/iss/securities/%s.json" % (securityString)
    moexData = json.loads(urllib.request.urlopen(query).read())
    rows = moexData["description"]["data"]

    if len(rows) == 0:
        return "Error: Bad security code.", 400

    db = utils.get_db()
    if not db.connected:
        return db.message, 500

    while not utils.working_day(requestDate):
        requestDate = requestDate - datetime.timedelta(days=1)
    requestDateString = requestDate.strftime("%Y-%m-%d")

    yearAwayDate = requestDate - datetime.timedelta(days=365)
    if yearAwayDate.date().year < 2015:
        yearAwayDate = datetime.date(2015, 1, 1)
    previousDate = requestDate - datetime.timedelta(days=1)
    weekAwayDate = requestDate - datetime.timedelta(days=7)
    nextDate = requestDate + datetime.timedelta(days=1)

    candles = [[], [], [], [], [], [], []]

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
            "high_price numeric(10, 4), " \
            "low_price numeric(10, 4), " \
            "volume integer, " \
            "CONSTRAINT epoch_interval_key PRIMARY KEY(epoch, interval));" % (securityString + "_candles") ):
            db.close()
            return "Can't create table '%s_candles'. %s" % (securityString + "_candles", db.error), 500
    else:
        # select candles for that date
        rows = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
            "FROM %s WHERE interval=1 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
            % (securityString + "_candles", utils.epoch(requestDate), utils.epoch(nextDate)))
        if rows is not None and len(rows) != 0:
            candles[0] = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
                "FROM %s WHERE interval=1 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
                % (securityString + "_candles", utils.epoch(previousDate), utils.epoch(requestDate)))
            candles[0].extend(rows)

            candles[3] = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
                "FROM %s WHERE interval=10 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
                % (securityString + "_candles", utils.epoch(weekAwayDate), utils.epoch(nextDate)))
            candles[6] = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
                "FROM %s WHERE interval=24 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
                % (securityString + "_candles", utils.epoch(yearAwayDate), utils.epoch(nextDate)))

            # process it all
            calculate_all_candles(candles)
            response = { "data": candles, "KC": [ema.keltner_channel(data).tolist() for data in candles] }

            db.close()
            return json.dumps(response, indent=4), 200

    # if DB has no data, get it from MOEX
    candles[0] = read_all_candles(securityString, previousDate.strftime("%Y-%m-%d"), requestDateString, 1)
    candles[1] = read_all_candles(securityString, weekAwayDate.strftime("%Y-%m-%d"), requestDateString, 10)
    candles[2] = read_all_candles(securityString, yearAwayDate.strftime("%Y-%m-%d"), requestDateString, 24)

    # write data to DB
    if not insert_candles(securityString, 1, candles[0], db):
        return "Can't write 1 min data to table '%s_candles'. %s" % (securityString + "_candles", db.error), 500
    if not insert_candles(securityString, 10, candles[1], db):
        return "Can't write 10 min data to table '%s_candles'. %s" % (securityString + "_candles", db.error), 500
    if not insert_candles(securityString, 24, candles[2], db):
        return "Can't write 1 day data to table '%s_candles'. %s" % (securityString + "_candles", db.error), 500

    # select candles for that date
    candles[0] = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
        "FROM %s WHERE interval=1 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
        % (securityString + "_candles", utils.epoch(previousDate), utils.epoch(nextDate)))
    candles[3] = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
        "FROM %s WHERE interval=10 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
        % (securityString + "_candles", utils.epoch(weekAwayDate), utils.epoch(nextDate)))
    candles[6] = db.select("SELECT epoch, open_price, high_price, low_price, close_price, volume " \
        "FROM %s WHERE interval=24 AND epoch>%d AND epoch<%d ORDER BY epoch;" \
        % (securityString + "_candles", utils.epoch(yearAwayDate), utils.epoch(nextDate)))

    # process it all
    calculate_all_candles(candles)
    response = { "data": candles, "KC": [ema.keltner_channel(data).tolist() for data in candles] }

    db.close()
    return json.dumps(response, indent=4), 200

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
    query = "INSERT INTO %s (epoch, trade_date, time_begin, interval, open_price, close_price, " \
        "high_price, low_price, volume) VALUES " % (securityString + "_candles")
    arguments = ','.join("(%d, '%s', '%s', %d, %.4f, %.4f, %.4f, %.4f, %d)" \
        % (utils.epoch_from_str(row[6]), row[6][:-9], row[6], interval, row[0], row[1], \
            row[2], row[3], row[5]) for row in candles)
    if not db.execute(query + arguments + " ON CONFLICT ON CONSTRAINT epoch_interval_key DO NOTHING;"):
        db.close()
        return False
    return True

def calculate_all_candles(candles):
    delta = time.timezone * 1000
    lastCandle = []
    candles[1] = []
    candles[2] = []
    candles0 = []
    for candle in candles[0]:
        candle = list(candle)
        candle[0] -= delta
        candles0.append(candle)
        if len(lastCandle) == 0:
            lastCandle = [ candle[:], candle[:] ]
        else:
            if candle[0] % 18000 == 0:
                candles[1].append(lastCandle[0][:])
                lastCandle[0] = candle[:]
            else:
                lastCandle[0][2] = max(candle[2], lastCandle[0][2])
                lastCandle[0][3] = min(candle[3], lastCandle[0][3])
                lastCandle[0][4] = candle[4]
                lastCandle[0][5] += candle[5]
            if candle[0] % 300000 == 0:
                candles[2].append(lastCandle[1][:])
                lastCandle[1] = candle[:]
            else:
                lastCandle[1][2] = max(candle[2], lastCandle[1][2])
                lastCandle[1][3] = min(candle[3], lastCandle[1][3])
                lastCandle[1][4] = candle[4]
                lastCandle[1][5] += candle[5]
    candles[1].append(lastCandle[0][:])
    candles[2].append(lastCandle[1][:])
    candles[0] = candles0

    lastCandle = []
    candles[4] = []
    candles[5] = []
    candles3 = []
    for candle in candles[3]:
        candle = list(candle)
        candle[0] -= delta
        candles3.append(candle)
        if len(lastCandle) == 0:
            lastCandle = [ candle[:], candle[:] ]
        else:
            if candle[0] % 1800000 == 0:
                candles[4].append(lastCandle[0][:])
                lastCandle[0] = candle[:]
            else:
                lastCandle[0][2] = max(candle[2], lastCandle[0][2])
                lastCandle[0][3] = min(candle[3], lastCandle[0][3])
                lastCandle[0][4] = candle[4]
                lastCandle[0][5] += candle[5]
            if candle[0] % 3600000 == 0:
                candles[5].append(lastCandle[1][:])
                lastCandle[1] = candle[:]
            else:
                lastCandle[1][2] = max(candle[2], lastCandle[1][2])
                lastCandle[1][3] = min(candle[3], lastCandle[1][3])
                lastCandle[1][4] = candle[4]
                lastCandle[1][5] += candle[5]
    candles[4].append(lastCandle[0][:])
    candles[5].append(lastCandle[1][:])
    candles[3] = candles3

    candles6 = []
    for candle in candles[6]:
        candle = list(candle)
        candle[0] -= delta
        candles6.append(candle)
    candles[6] = candles6

    