from app import app, utils, futures_candles
import urllib.request
from flask import request
import datetime
import simplejson as json
from time import sleep
import time

# A route to get candles data
# http://127.0.0.1:5000/api/v1/options/candles?sec=si_bd1a_bp1a&date=2021-04-01
@app.route('/api/v1/options/candles', methods=['GET'])
def api_options_candles():
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

        if requestDate.date().year < 2020:
            return "Year should be 2020 or more: '" + requestDateString + "'", 400
    except:
        return "Bad date format: '" + requestDateString + "'", 400
    if not utils.working_day(requestDate):
        return "'" + requestDateString + "' is not a working day", 400

    db = utils.get_db()
    if not db.connected:
        return db.message, 500

    option = db.select("SELECT * FROM options_by_date WHERE trade_date='%s' " \
        "AND secid='%s';" % (requestDateString, securityString))[0]
    if option is None or len(option) == 0:
        return "Error: Bad security code:"+securityString, 400

    nextDate = requestDate + datetime.timedelta(days=1)

    # check if table exists
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s);", \
        (option[1] + "_candles", ))
    if rows[0][0] == False:
        if not db.execute("CREATE TABLE %s (" \
            "epoch bigint, " \
            "trade_date character varying(10), " \
            "time_begin character varying(20), " \
            "name character varying(36), " \
            "type character varying(5), " \
            "strike numeric(10, 4), " \
            "open_price numeric(10, 4), " \
            "close_price numeric(10, 4), " \
            "high_price numeric(10, 4), " \
            "low_price numeric(10, 4), " \
            "volume integer, " \
            "PRIMARY KEY (epoch, type, strike));" % (option[1] + "_candles") ):
            db.close()
            return "Can't create table '%s'. %s" % (option[1] + "_candles", db.error), 500
    else:
        # select candles for that date
        rows = db.select("SELECT epoch, type, strike, open_price, high_price, low_price, close_price, volume " \
            "FROM %s WHERE type='%s' AND epoch>%d AND epoch<%d ORDER BY epoch;" \
            % (option[1] + "_candles", "CALL", utils.epoch(requestDate), utils.epoch(nextDate)))
        if rows is not None and len(rows) != 0:
            calls = dict()
            for row in rows:
                num = float(row[2])
                if num.is_integer():
                    strike = str(int(num))
                else:
                    strike = str(num)
                if strike in calls:
                    calls[strike].append(row[:])
                else:
                    calls[strike] = [row[:]]

            rows = db.select("SELECT epoch, type, strike, open_price, high_price, low_price, close_price, volume " \
                "FROM %s WHERE type='%s' AND epoch>%d AND epoch<%d ORDER BY epoch;" \
                % (option[1] + "_candles", "PUT", utils.epoch(requestDate), utils.epoch(nextDate)))
            puts = dict()
            for row in rows:
                num = float(row[2])
                if num.is_integer():
                    strike = str(int(num))
                else:
                    strike = str(num)
                if strike in puts:
                    puts[strike].append(row[:])
                else:
                    puts[strike] = [row[:]]

            candles, code = futures_candles.get_futures_candles(requestDate, option[3], db)
            if code != 200:
                return candles, code

            response = { "calls": calls, "puts": puts, "asset": candles[0] }

            

            db.close()
            return json.dumps(response), 200
    
    startTime = time.time()

    # if DB has no data, get it from MOEX
    delta = utils.strike_delta(option[1])
    high = (int(option[6] / delta) + 6) * delta
    low = max(int(option[7] / delta) - 5, 1) * delta
    stringArray = option[1].split('_')
    for strike in range(low, high, delta):
        if isinstance(strike, int) or strike.is_integer():
            strike_key = str(int(strike))
        else:
            strike_key = str(strike)
        callName = stringArray[0] + strike_key + stringArray[1]
        putName = stringArray[0] + strike_key + stringArray[2]

        candles = read_all_candles(callName, requestDateString, requestDateString)
        # write data to DB
        if len(candles) != 0:
            if not insert_candles(option[1], callName, "CALL", strike, candles, db):
                return "Can't write data to table '%s'. %s" % (option[1] + "_candles", db.error), 500

        candles = read_all_candles(putName, requestDateString, requestDateString)
        # write data to DB
        if len(candles) != 0:
            if not insert_candles(option[1], putName, "PUT", strike, candles, db):
                return "Can't write data to table '%s'. %s" % (option[1] + "_candles", db.error), 500

    # select candles for that date
    rows = db.select("SELECT epoch, type, strike, open_price, high_price, low_price, close_price, volume " \
        "FROM %s WHERE type='%s' AND epoch>%d AND epoch<%d ORDER BY epoch;" \
        % (option[1] + "_candles", "CALL", utils.epoch(requestDate), utils.epoch(nextDate)))
    calls = dict()
    for row in rows:
        num = float(row[2])
        if num.is_integer():
            strike = str(int(num))
        else:
            strike = str(num)
        if strike in calls:
            calls[strike].append(row[:])
        else:
            calls[strike] = [row[:]]

    rows = db.select("SELECT epoch, type, strike, open_price, high_price, low_price, close_price, volume " \
        "FROM %s WHERE type='%s' AND epoch>%d AND epoch<%d ORDER BY epoch;" \
        % (option[1] + "_candles", "PUT", utils.epoch(requestDate), utils.epoch(nextDate)))
    puts = dict()
    for row in rows:
        num = float(row[2])
        if num.is_integer():
            strike = str(int(num))
        else:
            strike = str(num)
        if strike in puts:
            puts[strike].append(row[:])
        else:
            puts[strike] = [row[:]]

    candles, code = futures_candles.get_futures_candles(requestDate, option[3], db)
    if code != 200:
        return candles, code

    app.logger.info("Got MOEX Option candles, elapsed time: %.3f" % (time.time()-startTime))

    response = { "calls": calls, "puts": puts, "asset": candles[0] }
    db.close()
    return json.dumps(response), 200

def read_all_candles(securityString, startDate, tillDate):
    index = 0
    count = 0
    candles = []
    while True:
        query = "https://iss.moex.com/iss/engines/futures/markets/options/securities/%s/" \
            "candles.json?from=%s&till=%s&interval=1&iss.meta=off" \
            "&start=%d" % (securityString, startDate, tillDate, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        dataRead = moexData["candles"]["data"]
        candles.extend(dataRead)
        index = len(candles)
        count += 1
        if len(dataRead) == 0 or count > 40:
            break
        sleep(0.10)
    return candles

def insert_candles(securityString, name, type, strike, candles, db):
    query = "INSERT INTO %s (epoch, trade_date, time_begin, name, type, strike, open_price, close_price, " \
        "high_price, low_price, volume) VALUES " % (securityString + "_candles")
    arguments = ','.join("(%d, '%s', '%s', '%s', '%s', %.4f, %.4f, %.4f, %.4f, %.4f, %d)" \
        % (utils.epoch_from_str(row[6]), row[6][:-9], row[6], name, type, strike, row[0], row[1], \
            row[2], row[3], row[5]) for row in candles)
    if not db.execute(query + arguments + " ON CONFLICT (epoch, type, strike) DO NOTHING;"):
        db.close()
        return False
    return True
