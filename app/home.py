from app import app, db_pg

@app.route('/', methods=['GET'])
def home():
    db = db_pg.get_db()
    if not db.connected:
        return db.message, 200
    else:
        db.close()
        return "Futures & Options Market Data DB API", 200
