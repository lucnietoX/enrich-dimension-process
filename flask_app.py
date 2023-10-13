import json
from flask import Flask, jsonify, request
import psycopg2
from psycopg2 import extras

app = Flask(__name__)
config = json.load(open("config.json"))["config"]
db_server = config["DB_SERVER"]
db_port = config["DB_PORT"]
db_user = config["DB_USER"]
db_pass = config["DB_PASS"]
db_name = config["DB_NAME"]
db_params = {
    "dbname": db_name,
    "user": db_user,
    "password": db_pass,
    "host": db_server
}

@app.route('/products', methods=['GET'])
def get_products():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
    cursor.execute('SELECT * FROM stg_product')
    products = cursor.fetchall()
    conn.close()
    return jsonify(products)

if __name__ == '__main__':
    app.run(debug=True)