from app import app, utils
import urllib.request
from flask import request
import datetime
import simplejson as json
from time import sleep
import math
import time

# A route to return all of the available options at date.
# http://127.0.0.1:5000/api/v1/options?q=Si&date=2021-04-01
@app.route('/api/v1/options', methods=['GET'])
def api_options():
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
    
    if not 'q' in request.args:
        return "Error: No query provided.", 400
    requestQuery = request.args['q'].lower()
    if len(requestQuery) != 2:
        return "Bad query provided, length should be equal 2.", 400

    db = utils.get_db()
    if not db.connected:
        return db.message, 500

    # check if table exists
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='options_by_date');")
    if rows[0][0] == False:
        if not db.execute("CREATE TABLE options_by_date (" \
            "trade_date character varying(10), " \
            "secid character varying(36), " \
            "name character varying(36), " \
            "asset_code character varying(36), " \
            "first_trade character varying(10), " \
            "expiration_date character varying(10), " \
            "strike_high numeric(10, 4), " \
            "strike_low numeric(10, 4)), " \
            "PRIMARY KEY (trade_date, secid));"):
            db.close()
            return "Can't create table OPTIONS_BY_DATE. " + db.error, 500
    else:
        # select options for that date
        rows = db.select("SELECT * FROM options_by_date WHERE trade_date='%s' " \
            "AND SUBSTRING(secid for 2)='%s' ORDER BY expiration_date;" % (requestDateString, requestQuery))
        if rows is not None and len(rows) != 0:
            db.close()
            app.logger.info("DB Options: Send %d records" % len(rows))
            return json.dumps(rows), 200

    startTime = time.time()

    # if DB has no data, get it from MOEX

    # 1. First we search for all options with requested base asset traded at requested date 
    # search page with our query, division by 2
    index = 0
    index_prev = 0
    searchBack = True
    while True:
        query = "https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json?date=%s" \
            "&iss.meta=off&start=%d" % (requestDateString, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        if len(moexData) == 0:
            db.close()
            app.logger.info("MOEX Options: Send 0 records")
            return json.dumps([]), 200
        if moexData["history"]["data"][-1][2][:2].upper() >= requestQuery.upper():
            searchBack = moexData["history"]["data"][0][2][:2].upper() >= requestQuery.upper()
            break
        if index + 100 >= moexData["history.cursor"]["data"][0][1]:
            searchBack = False
            break
        index_prev = index
        index = (index_prev + moexData["history.cursor"]["data"][0][1]) // 2
        sleep(0.10)

    # search for page starting from another security
    if searchBack:
        index_right = index_prev
        index_left = index
        while index_left - index_right > 100:
            index = (index_right + index_left) // 2
            query = "https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json?date=%s" \
                "&iss.meta=off&start=%d" % (requestDateString, index)
            moexData = json.loads(urllib.request.urlopen(query).read())
            if moexData["history"]["data"][0][2][:2].upper() < requestQuery.upper() and \
                moexData["history"]["data"][-1][2][:2].upper() >= requestQuery.upper():
                index_right = index
                break
            if moexData["history"]["data"][0][2][:2].upper() < requestQuery.upper():
                index_right = index
            else:
                index_left = index
            sleep(0.10)
        index = index_right

    rawData = []
    while True:
        query = "https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json?date=%s" \
            "&iss.meta=off&start=%d" % (requestDateString, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        rawData.extend(moexData["history"]["data"])
        index += moexData["history.cursor"]["data"][0][2]
        if moexData["history"]["data"][-1][2][:2].upper() > requestQuery.upper() or index >= moexData["history.cursor"]["data"][0][1]:
            break
        sleep(0.10)
        
    app.logger.info("Got %d MOEX Options, elapsed time: %.3f" % (len(rawData), time.time()-startTime))
    startTime = time.time()
    
    # 2. Now we compact found options
    # filter options
    optionsData = [row for row in rawData if row[0] is not None and row[9] != 0 and \
        row[2][:2].upper() == requestQuery.upper() and row[3] is not None and \
        row[4] is not None and row[5] is not None and row[6] is not None]

    # sort by volume, descending
    optionsData = sorted(optionsData, reverse=True, key=lambda x: x[9])
    codes = []
    # search for unique options
    for row in optionsData:
        if row[9] is None or row[9] == 0 or row[3] is None or row[4] is None or row[5] is None or row[6] is None:
            continue    
        if row[2][:2].upper() != requestQuery.upper():
            continue
        alreadyThere = False
        for code in codes:
            if similar_options(row[2], code):
                alreadyThere = True
                break
        if not alreadyThere:
            codes.append(row[2])

    # 3. Get option specifications from MOEX and high/low for base asset for given date
    weekAwayDate = requestDate - datetime.timedelta(days=7)
    weekAwayDateString = weekAwayDate.strftime("%Y-%m-%d")
    futures = ""
    optionsData = []
    for code in codes:
        query = "https://iss.moex.com/iss/securities/%s.json" % (code)
        moexData = json.loads(urllib.request.urlopen(query).read())
        row = moexData["description"]["data"]
        newFutures = utils.futures_code(row[2][2])
        if newFutures != futures:
            futures = newFutures
            query = "https://iss.moex.com/iss/engines/futures/markets/forts/securities/%s/" \
                "candles.json?from=%s&till=%s&interval=24&iss.meta=off" % (futures, weekAwayDateString, requestDateString)
            moexData = json.loads(urllib.request.urlopen(query).read())
            high = float(moexData["candles"]["data"][-1][2])
            low = float(moexData["candles"]["data"][-1][3])
            delta = utils.strike_delta(futures)
            high = int(math.ceil(high / delta) * delta)
            low = int(math.floor(low / delta) * delta)
        # trade_date, secid, name, asset_code, first_trade, expiration_date, strike_high, strike_low
        optionsData.append([requestDateString, options_name(code).lower(), row[2][2], utils.futures_code(row[2][2]), row[7][2], \
            row[5][2], high , low])

    app.logger.info("Found %d unique options, elapsed time: %.3f" % (len(optionsData), time.time()-startTime))
    
    # write data to DB
    needToCommit = False
    query = "INSERT INTO options_by_date(trade_date, secid, name, asset_code, first_trade, expiration_date, " \
        "strike_high, strike_low) VALUES "
    for row in optionsData:
        query += "('%s', '%s', '%s', '%s', '%s', '%s', %.4f, %.4f), " \
            % (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
        needToCommit = True
    query = query[:-2] + " ON CONFLICT (trade_date, secid) DO NOTHING;"

    if needToCommit:
        if not db.execute(query):
            db.close()
            return "Can't write to table OPTIONS_BY_DATE", 500

    optionsData = sorted(optionsData, key=lambda x: x[5])

    db.close()
    app.logger.info("MOEX Options: Send %d records" % len(optionsData))
    return json.dumps(optionsData), 200

def similar_options(option1, option2):
    if option1[:2] != option2[:2]:
        return False
    month = -3
    if not option1[-1].isdigit():
        if option1[-2:] != option2[-2:]:
            return False
        if option1[-4] != option2[-4]:
            return False
    else:
        if option1[-1] != option2[-1]:
            return False
        if option1[-3] != option2[-3]:
            return False
        month = -2
    if utils.get_option_month(option1[month]) != utils.get_option_month(option2[month]):
        return False
    return True

def options_name(code):
    if not code[-1].isdigit():
        month = utils.get_option_month(code[-3])
        return "".join([code[:2], "_", code[-4], utils.get_call_month(month), code[-2:], "_", \
            code[-4], utils.get_put_month(month), code[-2:]])
    else:
        month = utils.get_option_month(code[-2])
        return "".join([code[:2], "_", code[-3], utils.get_call_month(month), code[-1], "_", \
            code[-3], utils.get_put_month(month), code[-1]])
