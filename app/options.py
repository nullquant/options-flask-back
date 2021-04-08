from app import app, utils
import urllib.request
from flask import request
import datetime
import simplejson as json
from time import sleep
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
    requestQuery = request.args['q']
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
            "open_price numeric(10, 4), " \
            "low_price numeric(10, 4), " \
            "high_price numeric(10, 4), " \
            "close_price numeric(10, 4), " \
            "volume integer, " \
            "open_position integer);"):
            db.close()
            return "Can't create table OPTIONS_BY_DATE. " + db.error, 500
    else:
        # select options for that date
        rows = db.select("SELECT name FROM options_by_date WHERE trade_date='%s' " \
            "AND SUBSTRING(name for 2)='%s' GROUP BY name;" % (requestDateString, requestQuery))
        if rows is not None and len(rows) != 0:
            # sort by expiration date
            rows.sort(key=options_sort)
            db.close()
            app.logger.info("DB Options: Send %d records" % len(rows))
            return json.dumps(rows), 200

    startTime = time.time()

    # if DB has no data, get it from MOEX
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

    optionsData = []
    while True:
        query = "https://iss.moex.com/iss/history/engines/futures/markets/options/securities.json?date=%s" \
            "&iss.meta=off&start=%d" % (requestDateString, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        optionsData.extend(moexData["history"]["data"])
        index += moexData["history.cursor"]["data"][0][2]
        if moexData["history"]["data"][-1][2][:2].upper() > requestQuery.upper() or index >= moexData["history.cursor"]["data"][0][1]:
            break
        sleep(0.10)
        
    app.logger.info("MOEX Options, elapsed time:",time.time()-startTime)
    
    # write data to DB
    needToCommit = False
    query = "INSERT INTO options_by_date(trade_date, secid, name, open_price, low_price, high_price, " \
            "close_price, volume, open_position) VALUES "
    for row in optionsData:
        if row[9] is None or row[9] == 0 or row[3] is None or row[4] is None or row[5] is None or row[6] is None:
            continue    
        if row[2][:2].upper() != requestQuery.upper():
            continue
        query += "('%s', '%s', '%s', %.4f, %.4f, %.4f, %.4f, %d, %d), " \
            % (row[1], row[2], options_name(row[2]), row[3], row[4], row[5], row[6], row[9], row[10])
        needToCommit = True
    query = query[:-2] + ";"

    if needToCommit:
        if not db.execute(query):
            db.close()
            return "Can't write to table OPTIONS_BY_DATE", 500

    # select options for that date
    rows = db.select("SELECT name FROM options_by_date WHERE trade_date='%s' " \
        "AND SUBSTRING(name for 2)='%s' GROUP BY name;" % (requestDateString, requestQuery))
    # sort by expiration date
    rows.sort(key=options_sort)

    db.close()
    app.logger.info("MOEX Options: Send %d records" % len(rows))
    return json.dumps(rows), 200

def options_name(name):
    monthLetter = name[-2] if name[-1].isdigit() else name[-3]
    month = {
        'A': '-1.',
        'B': '-2.',
        'C': '-3.',
        'D': '-4.',
        'E': '-5.',
        'F': '-6.',
        'G': '-7.',
        'H': '-8.',
        'I': '-9.',
        'J': '-10.',
        'K': '-11.',
        'L': '-12.',
        'M': '-1.',
        'N': '-2.',
        'O': '-3.',
        'P': '-4.',
        'Q': '-5.',
        'R': '-6.',
        'S': '-7.',
        'T': '-8.',
        'U': '-9.',
        'V': '-10.',
        'W': '-11.',
        'X': '-12.'
    }.get(monthLetter, '.')
    if name[-1].isdigit():
        return name[:2] + month + '2' + name[-1]
    elif name[-1] == 'A':
        return name[:2] + "-1w" + month[1:] + '2' + name[-2]
    else:
        return name[:2] + "-2w" + month[1:] + '2' + name[-2]

def options_sort(row):
    name = row[0]
    date = name.split('-')[1]
    key = 0
    if 'w' in date:
        key = (int(date[0]) - 3) * 0.1
        date = date.split('w')[1]
    key += int(date.split('.')[0])
    key += int(date[-1]) * 12
    return key
