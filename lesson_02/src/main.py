"""Flask application for extracting and processing sales data.

This module sets up a Flask web server with an endpoint to handle
POST requests for extracting sales data. It utilizes the `fetch_sales_data`
function from the `job_1` module to retrieve data from a specified directory.

Endpoints:
- POST /: Extracts sales data based on the provided `raw_dir` parameter.

Usage:
- Run the module to start the Flask server on specified ports.
"""

from flask import Flask, request, jsonify
from job_1 import fetch_sales_data
app = Flask(__name__)


@app.route('/', methods=['POST'])
def extract_sales_data():
    """Extract sales data from POST request.

    :return: message: str, http code
    """
    # Get data from request
    data = request.get_json()
    raw_dir = data.get('raw_dir')

    try:
        fetch_sales_data(raw_dir)
        return jsonify({"message": "Sales data extracted successfully!"}), 201
    except Exception as e:
        return jsonify({"message": str(e)}), 500


if __name__ == '__main__':
    app.run(port=8081)  # For get
