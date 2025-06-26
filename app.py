from flask import Flask, request, jsonify

import os

# Create Flask app
app = Flask(__name__)


@app.route('/')
def home():
    return "Hello, Flask!"

@app.route('/api/hello', methods=['GET'])
def hello_api():
    return jsonify({"message": "Hello from Flask API!"}), 200


if __name__ == '__main__':
    app.run(debug=True)
