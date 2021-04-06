from flask import Flask
from flask_restful import Api

app = Flask(__name__)
api = Api(app)

from app import home
from app import futures
from app import security
from app import futures_candles

if __name__ == '__main__':
    app.run()
