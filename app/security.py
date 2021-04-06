from app import app, utils
import urllib.request
from flask import request
import simplejson as json

# A route to get security data.
# http://127.0.0.1:5000/api/v1/security?sec=SiH1
@app.route('/api/v1/security', methods=['GET'])
def api_security():
    if not 'sec' in request.args:
        return "Error: No security code provided.", 404
    securityString = request.args['sec']

    db = utils.get_db()
    if not db.connected:
        return db.message, 200

    # check if table exists
    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='securities');")
    if rows[0][0] == False:
        if not db.execute("CREATE TABLE securities (" \
            "secid character varying(36), " \
            "type_name character varying(10), " \
            "asset_code character varying(36), " \
            "name character varying(100), " \
            "first_trade character varying(10), " \
            "last_trade character varying(10), " \
            "expiration character varying(10));"):
            db.close()
            return "Can't create table SECURITIES. " + db.error, 200
    else:
        # select security
        rows = db.select("SELECT * FROM securities WHERE secid=%s;",(securityString,))
        if len(rows) == 0:
            db.close()
            return json.dumps(rows), 200

    # if DB has no data, get it from MOEX
    query = "https://iss.moex.com/iss/securities/%s.json" % (securityString)
    moexData = json.loads(urllib.request.urlopen(query).read())
    rows = moexData["description"]["data"]

    # MOEX has no such security?
    if len(rows) == 0:
        db.close()
        return "Error: Bad security code.", 404

    # write data to DB
    query = "INSERT INTO securities(secid, type_name, asset_code, name, first_trade, " \
            "last_trade, expiration) VALUES "
    query += "('%s', '%s', '%s', '%s', '%s', '%s', '%s');" \
                % (rows[0][2], rows[10][2], rows[7][2], rows[3][2], rows[4][2], rows[5][2], rows[6][2])

    if not db.execute(query):
        db.close()
        return "Can't write to table SECURITIES" + db.error, 200

    # select security
    rows = db.select("SELECT * FROM securities WHERE secid=%s;",(securityString,))
    db.close()
    return json.dumps(rows), 200
