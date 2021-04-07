from app import app, utils

@app.route('/', methods=['GET'])
def home():
    db = utils.get_db()
    if not db.connected:
        return db.message, 200
    else:
        db.close()
        return "Futures & Options Market Data DB API", 200
