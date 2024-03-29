from app import app, utils, futures_candles, gbs
from flask import request
import datetime
import simplejson as json
import math
import numpy as np

# A route to get option tables data
# http://127.0.0.1:5000/api/v1/options/tables?sec=si_bd1d_bp1d&asset=sim1&date=2021-04-14
@app.route('/api/v1/options/tables', methods=['GET'])
def api_options_tables():
    if not 'sec' in request.args:
        return "Error: No security code provided.", 400
    optionString = request.args['sec'].lower()
    if not 'asset' in request.args:
        return "Error: No base asset code provided.", 400
    baString = request.args['asset'].lower()
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

    tableName = utils.futures_name(baString.upper()).lower().replace('-','_').replace('.','_')+'_options'

    rows = db.select("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s);", \
            (tableName, ))
    if rows[0][0] == False:
        db.close()
        return "DB table '%s' not found" % tableName, 500

    stringArray = optionString.split('_')
    callMatch = stringArray[0] + "%" + stringArray[1]
    putMatch = stringArray[0] + "%" + stringArray[2]

    option_data = []

    query = "SELECT * FROM %s WHERE LOWER(code) LIKE '%s' AND trade_date='%s' ORDER BY epoch;" \
        % (tableName, callMatch, requestDateString)
    rowCalls = db.select(query)
    option_data.extend(rowCalls)

    query = "SELECT * FROM %s WHERE LOWER(code) LIKE '%s' AND trade_date='%s' ORDER BY epoch;" \
        % (tableName, putMatch, requestDateString)
    rowPuts = db.select(query)
    option_data.extend(rowPuts)

    option_data.sort(key=lambda x:x[0])

    candles, code = futures_candles.get_futures_candles(requestDate, baString, db)
    if code != 200:
        db.close()
        return candles, code
    asset = candles[0]

    query = "SELECT * FROM iv_history WHERE SUBSTRING(LOWER(asset) for 2) = '%s';" \
        % (optionString[:2])
    iv_history = db.select(query)
    if iv_history is None or len(iv_history) == 0:
        db.close()
        return "Option '%s' IV history is not found" % optionString, 500
    db.close()

    historical_volatility = []
    sqrt_year = math.sqrt(252) * 100
    for i in range(21, len(candles[6])):
        sum_ccl2 = 0
        sum2_ccl = 0
        for j in range(21):
            ccl = math.log(candles[6][i - 20 + j][4] / candles[6][i - 20 + j - 1][4])
            sum_ccl2 = sum_ccl2 + ccl * ccl
            sum2_ccl = sum2_ccl + ccl
        historical_volatility.append([candles[6][i][0], \
                    math.sqrt(sum_ccl2 / 19.0 - sum2_ccl * sum2_ccl / 19.0 / 20.0) * sqrt_year])

    iv_history = np.array(iv_history)
    iv_history = iv_history[iv_history[:, 0].argsort()]
    expiration_dates = list(set([date for date in iv_history[:,2]]))
    expiration_dates.sort()
    week2 = []
    week14 = []    
    for exp_date in expiration_dates:
        option = iv_history[iv_history[:,2] == exp_date]
        option = option[option[:, 0].argsort()]
        if option[0, 0] == iv_history[0][0]:
            continue
        duration = option[0][3]
        if duration < 17:
            for row in option:
                if row[0] < requestDateString:
                    week2.append([utils.epoch_from_str(row[0], "%Y-%m-%d"), row[4], row[3]])
            week2.append([utils.epoch_from_str(row[0], "%Y-%m-%d")+1000, 'NaN', 0])
        elif duration < 130:
            for row in option:
                if row[0] < requestDateString:
                    week14.append([utils.epoch_from_str(row[0], "%Y-%m-%d"), row[4], row[3]])
            week14.append([utils.epoch_from_str(row[0], "%Y-%m-%d")+1000, 'NaN', 0])
    

    delta = utils.strike_delta(optionString)
    response = []
    index = 0
    last_price = {'CALL': {}, 'PUT': {}}
    while(index < len(option_data)):
        epoch = option_data[index][0]

        while(True):
            strike = num_string(option_data[index][5])
            #  6    7     8       9      10  11
            # bid, ask, last, last_time, oi, iv
            arr = [num_string(option_data[index][6]), num_string(option_data[index][7]), \
                num_string(option_data[index][8]), num_string(option_data[index][9]), \
                num_string(option_data[index][10]), num_string(option_data[index][11])]
            if arr[0] == '188888':
                arr[0] = ''
            if arr[1] == '188888':
                arr[1] = ''
            last_price[option_data[index][4]][strike] = arr
                
            if index+1 == len(option_data) or option_data[index+1][0] > epoch:
                break
            index = index + 1
            
        asset_price, asset_index = find_asset_price(epoch, asset)
        call_itm_price = math.floor(asset_price / delta) * delta
        put_itm_price = math.ceil(asset_price / delta) * delta
        put_start = 1 if call_itm_price == put_itm_price else 0

        option_table = []
        for i in range(-9, 11 + put_start):
            strike_price = call_itm_price + i * delta
            strike = num_string(strike_price)
            
            last_call = last_price['CALL'].get(strike)
            if last_call is None:
                last_call = ['', '', '', '', '', '']
            last_put = last_price['PUT'].get(strike)
            if last_put is None:
                last_put = ['', '', '', '', '', '']

            if strike_price == call_itm_price and strike_price == put_itm_price:
                itm = "CALL&PUT"
            elif call_itm_price + i * delta <= call_itm_price:
                itm = "CALL"
            else:
                itm = "PUT"
            
            if len(last_call[0]) != 0 and len(last_call[1]) != 0:
                spread = float(last_call[1]) - float(last_call[0])
                call_mid = (float(last_call[1]) + float(last_call[0])) / 2.0
                if call_mid != 0:
                    call_mid = "%s (%d%%)" % (utils.round_price(optionString, call_mid), \
                        int(round(spread * 100.0 / call_mid)))
                else:
                    call_mid = ''
            else:
                call_mid = ''

            if len(last_put[0]) != 0 and len(last_put[1]) != 0:
                spread = float(last_put[1]) - float(last_put[0])
                put_mid = (float(last_put[1]) + float(last_put[0])) / 2.0
                if put_mid != 0:
                    put_mid = "%s (%d%%)" % (utils.round_price(optionString, put_mid), \
                        int(round(spread * 100.0 / put_mid)))
                else:
                    put_mid = ''
            else:
                put_mid = ''
                
            option_table.append({"strike": strike, 
                                "call_bid": last_call[0],
                                "call_mid": call_mid,
                                "call_ask": last_call[1],
                                "call_last": last_call[2],
                                "call_last_time": utils.epoch_from_str(last_call[3], '%d.%m.%y %H:%M:%S'),
                                "call_oi": last_call[4], 
                                "call_iv": last_call[5], 
                                "put_bid": last_put[0],
                                "put_mid": put_mid,
                                "put_ask": last_put[1],
                                "put_last": last_put[2],
                                "put_last_time": utils.epoch_from_str(last_put[3], '%d.%m.%y %H:%M:%S'),
                                "put_oi": last_put[4], 
                                "put_iv": last_put[5], 
                                "itm": itm })
        response.append({"epoch":epoch, "asset": asset_price, "option_table": option_table[:]})
        index += 1    
    response = [response, historical_volatility, [week2, week14]]
    return json.dumps(response), 200


# epoch, open_price, high_price, low_price, close_price, volume
def find_asset_price(epoch, asset):
    index_left = 0
    index_right = len(asset)-1
    if epoch < asset[0][0]:
        return asset[0][1], 0 # open
    if epoch > asset[index_right][0]:
        return asset[index_right][4], index_right # close
    while(index_right - index_left > 1):
        index = (index_left + index_right) // 2
        if epoch < asset[index][0]:
            index_right = index
        elif epoch == asset[index][0]:
            return asset[index][1], index # open
        else:
            index_left = index
    if epoch == asset[index_right][0]:
        return asset[index_right][1], index_right # open
    elif epoch == asset[index_left][0]:
        return asset[index_left][1], index_left # open
    elif epoch > asset[index_left][0] + 60000:
        return asset[index_left][4], index_left # close
    return asset[index_left][1], index_left # open

def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def num_string(input_string):
    if isinstance(input_string, str) and not is_number(input_string):
        return input_string
    num = float(input_string)
    if num.is_integer():
        return str(int(num))
    elif input_string[-2:] == '00':
        return "%.2f" % num
    else:
        return "%.4f" % num

