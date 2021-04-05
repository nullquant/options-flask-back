from app import app
import os
import psycopg2
from urllib.parse import urlparse

#url = urlparse(os.environ.get('DATABASE_URL'))
#db = "dbname=%s user=%s password=%s host=%s " % (url.path[1:], url.username, url.password, url.hostname)
#schema = "schema.sql"
#conn = psycopg2.connect(db)
#cur = conn.cursor()

@app.route('/', methods=['GET'])
def home():
    return "Futures & Options Market Data DB API"

# A route to return all of the available entries in our catalog.
@app.route('/api/v1/resources/books/all', methods=['GET'])
def api_all():
    return "all"#jsonify(books)

@app.route('/api/v1/resources/books', methods=['GET'])
def api_id():
    # Check if an ID was provided as part of the URL.
    # If ID is provided, assign it to a variable.
    # If no ID is provided, display an error in the browser.
    #if 'id' in request.args:
    #    id = int(request.args['id'])
    #else:
    #    return "Error: No id field provided. Please specify an id."

    # Create an empty list for our results
    results = []

    # Loop through the data and match results that fit the requested ID.
    # IDs are unique, but other fields might return many results
    #for book in books:
    #    if book['id'] == id:
    #        results.append(book)

    # Use the jsonify function from Flask to convert our list of
    # Python dictionaries to the JSON format.
    return jsonify(results)


if __name__ == '__main__':
    app.run()
