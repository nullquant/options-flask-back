from flask import Flask
from flask_cors import CORS
from flask_restful import Api
import logging

app = Flask(__name__)
CORS(app)
api = Api(app)

logging.basicConfig(level=logging.DEBUG)

from app import home
from app import futures
from app import options
from app import security
from app import futures_candles
from app import option_candles

if __name__ == '__main__':
    app.run()
