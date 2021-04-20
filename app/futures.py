from app import app, utils
import urllib.request
from flask import request
import datetime
import simplejson as json
from time import sleep

# A route to return all of the available futures at date.
# http://127.0.0.1:5000/api/v1/futures?date=2021-04-01&q=Si
@app.route('/api/v1/futures', methods=['GET'])
def api_futures():
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
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='futures_by_date');")
    if rows[0][0] == False:
        if not db.execute("CREATE TABLE futures_by_date (" \
            "trade_date character varying(10), " \
            "secid character varying(36), " \
            "name character varying(36), " \
            "open_price numeric(10, 4), " \
            "low_price numeric(10, 4), " \
            "high_price numeric(10, 4), " \
            "close_price numeric(10, 4), " \
            "volume integer, " \
            "open_position integer, " \
            "PRIMARY KEY (trade_date, secid));"):
            db.close()
            return "Can't create table FUTURES_BY_DATE. " + db.error, 500
    else:
        # select futures for that date
        rows = db.select("SELECT secid, name FROM futures_by_date WHERE trade_date='%s' " \
            "AND SUBSTRING(name for 2)='%s';" % (requestDateString, requestQuery))
        if rows is not None and len(rows) != 0:
            # sort by expiration date
            rows.sort(key=futures_sort)
            db.close()
            app.logger.info("DB Futures: Send %d records" % len(rows))
            return json.dumps(rows), 200

    # if DB has no data, get it from MOEX
    index = 0
    count = 0
    futuresData = []
    while True:
        query = "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?date=%s" \
            "&iss.meta=off&start=%d" % (requestDateString, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        if len(moexData) == 0:
            db.close()
            app.logger.info("MOEX Futures: Send 0 records")
            return json.dumps([]), 200
        futuresData.extend(moexData["history"]["data"])
        index += moexData["history.cursor"]["data"][0][2]
        count += 1
        if index >= moexData["history.cursor"]["data"][0][1] or count > 100:
            break
        sleep(0.10)
        
    app.logger.info("Got %d records from MOEX" % len(futuresData))

    # write data to DB
    needToCommit = False
    query = "INSERT INTO futures_by_date(trade_date, secid, name, open_price, low_price, high_price, " \
            "close_price, volume, open_position) VALUES "
    count = 0
    for row in futuresData:
        if row[9] is None or row[9] == 0 or row[3] is None or row[4] is None or row[5] is None or row[6] is None:
            continue    
        if 'Si' in row[2] or 'BR' in row[2] or 'RI' in row[2]:
            query += "('%s', '%s', '%s', %.4f, %.4f, %.4f, %.4f, %d, %d), " \
                % (row[1], row[2], utils.futures_name(row[2]), row[3], row[4], row[5], row[6], row[9], row[10])
            count += 1
            needToCommit = True
    query = query[:-2] + " ON CONFLICT (trade_date, secid) DO NOTHING;"

    app.logger.info("Insert %d records in FUTURES_BY_DATE" % count)

    if needToCommit:
        if not db.execute(query):
            db.close()
            return "Can't write to table FUTURES_BY_DATE", 500

    # select futures for that date
    rows = db.select("SELECT secid, name FROM futures_by_date WHERE trade_date='%s' " \
        "AND SUBSTRING(secid for 2)='%s';" % (requestDateString, requestQuery))
    # sort by expiration date
    if rows is not None and len(rows) != 0:
        rows.sort(key=futures_sort)
    db.close()
    app.logger.info("MOEX Futures: Send %d records" % len(rows))
    return json.dumps(rows), 200

def futures_sort(row):
    name = row[1]
    date = name.split('-')[1]
    key = int(date.split('.')[0])
    key += int(date[-1]) * 12
    return key
