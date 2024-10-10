import os
import json
import shutil
import unittest
from job_2 import app, convert_json_to_avro

class TestConvertJSONToAvro(unittest.TestCase):

    def setUp(self):
        """Set up a test client and temporary directories."""
        self.app = app
        self.client = self.app.test_client()  # Flask's test client
        self.app.config['TESTING'] = True

        # Create temporary directories for raw and staging files
        self.raw_dir = 'temp_raw'
        self.stg_dir = 'temp_staging'

        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.stg_dir, exist_ok=True)

    def tearDown(self):
        """Remove temporary directories after each test."""
        if os.path.exists(self.raw_dir):
            shutil.rmtree(self.raw_dir)

    def test_convert_json_to_avro(self):
        """Test JSON to Avro conversion function."""
        # Create sample JSON data
        sample_json = [
            {"client": "Sean Davis", "purchase_date": "2022-08-09", "product": "Laptop", "price": 957},
            {"client": "Sean Davis", "purchase_date": "2022-08-09", "product": "Phone", "price": 1139},
        ]
        json_file_path = os.path.join(self.raw_dir, 'sales.json')

        with open(json_file_path, 'w') as json_file:
            json.dump(sample_json, json_file)

        # Call the function to convert JSON to Avro
        convert_json_to_avro(self.raw_dir, self.stg_dir)

        # Check if the Avro file is created in the staging directory
        avro_file_name = 'sales.avro'
        avro_file_path = os.path.join(self.stg_dir, avro_file_name)
        self.assertTrue(os.path.exists(avro_file_path))

    def test_handle_convert_request(self):
        """Test POST / endpoint for converting JSON to Avro."""
        # Create sample JSON data
        sample_json = [
            {"client": "Sean Davis", "purchase_date": "2022-08-09", "product": "Laptop", "price": 957},
            {"client": "Sean Davis", "purchase_date": "2022-08-09", "product": "Phone", "price": 1139},
        ]
        json_file_path = os.path.join(self.raw_dir, 'sales.json')

        with open(json_file_path, 'w') as json_file:
            json.dump(sample_json, json_file)

        # Send POST request to the Flask endpoint
        response = self.client.post('/', json={
            'raw_dir': self.raw_dir,
            'stg_dir': self.stg_dir
        })

        # Check the response and status code
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json, {'message': 'JSON files converted to Avro.'})

        # Check if the Avro file is created
        avro_file_path = os.path.join(self.stg_dir, 'sales.avro')
        self.assertTrue(os.path.exists(avro_file_path))

    def test_missing_params(self):
        """Test POST / with missing parameters."""
        # Send POST request without parameters
        response = self.client.post('/', json={})

        # Check response status code and error message
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json, {'error': 'raw_dir and stg_dir are required.'})


if __name__ == "__main__":
    unittest.main()
