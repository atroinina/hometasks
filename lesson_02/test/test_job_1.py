import unittest
from unittest.mock import patch
from job_1 import fetch_sales_data
import os
import shutil

class TestFetchSalesData(unittest.TestCase):


    def setUp(self):
        # Create a test directory before each test
        self.raw_dir = 'test/raw'
        os.makedirs(self.raw_dir, exist_ok=True)

    def tearDown(self):
        # Remove the test directory after each test
        if os.path.exists(self.raw_dir):
            shutil.rmtree(self.raw_dir)  # Remove the directory and its contents

    @patch('job_1.get_authorization_key')
    @patch('job_1.requests.get')
    def test_fetch_sales_data(self, mock_get, mock_get_authorization_key):
        mock_get_authorization_key.return_value = "test_key"

        # Use side_effect to change return values dynamically
        mock_get.side_effect = [
            # First call returns a successful response
            MockResponse(200, {'data': 'first page'}),
            # Second call returns another successful response
            MockResponse(200, {'data': 'second page'}),
            # Final call returns a 404 response to simulate end of data
            MockResponse(404, {})
        ]

        # Call the function to test
        try:
            fetch_sales_data(self.raw_dir)
        except Exception as e:
            self.fail(f"fetch_sales_data raised Exception {e} unexpectedly!")
        self.assertEqual(len(os.listdir(self.raw_dir)), 2)

    @patch('job_1.get_authorization_key')
    @patch('job_1.requests.get')
    def test_fetch_sales_data_error(self, mock_get, mock_get_authorization_key):
        mock_get_authorization_key.return_value = "test_key"

        # Simulate a server error on the first call
        mock_get.side_effect = [
            MockResponse(500, {}),  # Simulate server error
            MockResponse(404, {})  # Simulate end of data
        ]

        # Call the function to test and expect it to raise an exception
        with self.assertRaises(Exception) as context:
            fetch_sales_data(self.raw_dir)

        # Assert that the exception message is as expected
        self.assertIn("Error fetching data: 500", str(context.exception))

    @patch('job_1.get_authorization_key')
    @patch('job_1.requests.get')
    def test_fetch_sales_file_not_found(self, mock_get, mock_get_authorization_key):
        mock_get_authorization_key.return_value = "test_key"

        # Simulate a 404 response from the API
        mock_get.return_value.status_code = 404

        # Call the function to test and expect it to raise an Exception
        with self.assertRaises(Exception) as context:
            fetch_sales_data(self.raw_dir)

        # Assert that the exception message is as expected
        self.assertIn("Error fetching data: 404", str(context.exception))


# Helper class to mock the response from requests.get
class MockResponse:
    def __init__(self, status_code, json_data):
        self.status_code = status_code
        self._json_data = json_data

    def json(self):
        return self._json_data


if __name__ == '__main__':
    unittest.main()
