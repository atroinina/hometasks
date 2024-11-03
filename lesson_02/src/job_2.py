"""
Flask application for converting JSON files to Avro format.

This module provides an endpoint to convert JSON files from a specified
raw directory to Avro format and save them in a staging directory (job 2).
It uses Fastavro for the conversion and requires an Avro schema file.

Functions:
    - convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:
        Converts JSON files to Avro format.

    - handle_convert_request():
        Flask endpoint to handle conversion requests.
"""

import os
import json
import fastavro
from fastavro.schema import load_schema
from flask import Flask, request, jsonify
from lesson_02.src.util import get_base_dir
from util import clear_directory
app = Flask(__name__)


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:
    """
    Convert JSON files in all subdirectories of raw_dir to Avro format
    and save them in the stg directory, preserving folder structure.

    Parameters:
    raw_dir (str): The base directory containing JSON files organized by day.
    stg_dir (str): The directory where Avro files will be saved.
    """
    # Ensure stg_dir exists and is empty
    os.makedirs(stg_dir, exist_ok=True)
    clear_directory(stg_dir)
    path_to_schema = os.path.join(get_base_dir(), 'sales_schema.avsc')
    schema = load_schema(path_to_schema)  # Define Avro schema

    # Traverse all directories within raw_dir
    for root, _, files in os.walk(raw_dir):
        for json_file_name in files:
            if json_file_name.endswith('.json'):
                json_file_path = os.path.join(root, json_file_name)

                # Preserve directory structure in stg_dir
                relative_path = os.path.relpath(root, raw_dir)
                target_dir = os.path.join(stg_dir, relative_path)
                os.makedirs(target_dir, exist_ok=True)

                # Convert JSON to Avro
                with open(json_file_path, 'r') as json_file:
                    data = json.load(json_file)

                avro_file_name = json_file_name.replace('.json', '.avro')
                avro_file_path = os.path.join(target_dir, avro_file_name)

                # Save Avro file
                with open(avro_file_path, 'wb') as avro_file:
                    fastavro.writer(avro_file, schema, data)

                print(f"Converted {json_file_path} to Avro: {avro_file_path}")


@app.route('/', methods=['POST'])
def handle_convert_request():
    """
    Endpoint to handle the conversion job.

    This function takes a POST request
    with two parameters: raw_dir and stg_dir,
    reads the JSON files from raw_dir, converts them to Avro,
    and saves them in stg_dir.
    """
    req_data = request.get_json()

    raw_dir = req_data.get('raw_dir')
    stg_dir = req_data.get('stg_dir')

    if not raw_dir or not stg_dir:
        return jsonify({'error': 'raw_dir and stg_dir are required.'}), 400

    convert_json_to_avro(raw_dir, stg_dir)

    return jsonify(message='JSON files converted to Avro.'), 201


if __name__ == "__main__":
    app.run(host='localhost', port=8082)
