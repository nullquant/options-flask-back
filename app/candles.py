from app import app, db_pg
import urllib.request
from flask import request
import datetime
import simplejson as json



#
#// settings[0] = security, settings[1] = scale, settings[2] = start date, settings[3] = end date
#const query =
#        "https://iss.moex.com/iss/engines/futures/markets/forts/securities/" +
#        settings[0] +
#        "/candles.json?from=" +
#        settings[2] +
#        "&till=" +
#        settings[3] +
#        "&interval=" +
#        settings[1] +
#        "&iss.meta=off";

# A route to get candles data
# http://127.0.0.1:5000/api/v1/candles?sec=SiH1&date=2021-03-13
@app.route('/api/v1/candles', methods=['GET'])
def api_candles():
    if not 'sec' in request.args:
        return "Error: No security code provided.", 404
    securityString = request.args['sec']
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

    db = db_pg.get_db()
    if not db.connected:
        return db.message, 200

    # check if table exists
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='candles');")
    if rows[0][0] == False:
        if not db.execute("CREATE TABLE candles (" + \
            "trade_date character varying(10), " + \
            "secid character varying(36), " + \
            "open_price numeric(10, 4), " + \
            "low_price numeric(10, 4), " + \
            "high_price numeric(10, 4), " + \
            "close_price numeric(10, 4), " + \
            "volume integer, " + \
            "open_position integer);"):
            db.close()
            return "Can't create table FUTURES_BY_DATE" + db.error, 200
    else:
        # select futures for that date
        rows = db.select("SELECT * FROM futures_by_date WHERE trade_date=%s",(requestDateString,))
        if len(rows) != 0:
            db.close()
            return json.dumps(rows), 200

    # if DB has no data, get it from MOEX
    index = 0
    count = 0
    futuresData = []
    while True:
        query = "https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?date=%s" \
            "&iss.meta=off&start=%d" % (requestDateString, index)
        moexData = json.loads(urllib.request.urlopen(query).read())
        futuresData.extend(moexData["history"]["data"])
        index += moexData["history.cursor"]["data"][0][2]
        count += 1
        if index >= moexData["history.cursor"]["data"][0][1] or count > 10:
            break
        
    # write data to DB
    needToCommit = False
    query = "INSERT INTO futures_by_date(trade_date, secid, open_price, low_price, high_price, " \
            "close_price, volume, open_position) VALUES "
    for row in futuresData:
        if row[9] is None or row[9] == 0:
            continue    
        query += "('%s', '%s', %.4f, %.4f, %.4f, %.4f, %d, %d), " \
                % (row[1], row[2], row[3], row[4], row[5], row[6], row[9], row[10])
        needToCommit = True
    query = query[:-2] + ";"

    if needToCommit:
        if not db.execute(query):
            db.close()
            return "Can't write to table FUTURES_BY_DATE", 200

    # select futures for that date
    rows = db.select("SELECT * FROM futures_by_date WHERE trade_date=%s",(requestDateString,))
    db.close()
    return json.dumps(rows), 200
