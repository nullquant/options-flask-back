from app import app, db_pg
import urllib.request
from flask import request
import datetime
import simplejson as json

# A route to get security data.
# http://127.0.0.1:5000/api/v1/security?sec=SiH1
@app.route('/api/v1/security', methods=['GET'])
def api_security():
    if not 'sec' in request.args:
        return "Error: No security code provided.", 404
    securityString = request.args['sec']

    db = db_pg.get_db()
    if not db.connected:
        return db.message, 200

    # check if table exists
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='securities');")
    if rows[0][0] == False:
        try:
            db.cursor.execute("CREATE TABLE securities (" + \
            "secid character varying(36), " + \
            "type_name character varying(10), " + \
            "asset_code character varying(36), " + \
            "name character varying(100), " + \
            "first_trade character varying(10), " + \
            "last_trade character varying(10), " + \
            "expiration character varying(10));")
        except:
            db.close()
            return "Can't create table SECURITIES" + db.error, 200
        db.connection.commit()
    else:
        # select security
        rows = db.select("SELECT * FROM securities WHERE secid=%s",(securityString,))
        if len(rows) != 0:
            db.close()
            return json.dumps(rows), 200

    # if DB has no data, get it from MOEX
    query = "https://iss.moex.com/iss/securities/%s.json" % (securityString)
    moexData = json.loads(urllib.request.urlopen(query).read())
    row = moexData["description"]["data"]

    # write data to DB
    query = "INSERT INTO securities(secid, type_name, asset_code, name, first_trade, " \
            "last_trade, expiration) VALUES "
    query += "('%s', '%s', '%s', '%s', '%s', '%s', '%s');" \
                % (row[0][2], row[10][2], row[7][2], row[3][2], row[4][2], row[5][2], row[6][2])

    try:
        db.cursor.execute(query)
    except:
        db.close()
        return "Can't write to table SECURITIES" + db.error, 200
    db.connection.commit()

    # select security
    rows = db.select("SELECT * FROM securities WHERE secid=%s",(securityString,))
    db.close()
    return json.dumps(rows), 200


