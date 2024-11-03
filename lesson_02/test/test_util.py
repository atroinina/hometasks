import os
import shutil
import tempfile
import unittest
from unittest.mock import patch
from util import get_authorization_key, get_base_dir, clear_directory


class TestUtil(unittest.TestCase):
    @patch('os.getenv')
    def test_get_authorization_key(self, mock_env):
        """Test that checks if authorization key is set correctly."""
        mock_env.return_value = 'test'
        result = get_authorization_key()
        self.assertEqual(result, 'test')

        mock_env.return_value = None
        with self.assertRaises(EnvironmentError):
            get_authorization_key()

    @patch('os.getenv')
    def test_get_base_dir(self, mock_env):
        """Test that checks if base_dir is set correctly."""
        mock_env.return_value = 'test'
        result = get_base_dir()
        self.assertEqual(result, 'test')
        mock_env.return_value = None
        with self.assertRaises(EnvironmentError):
            get_base_dir()

    def setUp(self):
        # Create temp directory for testing clearDirectory()
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Delete the directory after the test
        shutil.rmtree(self.test_dir)

    def test_clean_directory_files_removed(self):
        """"""
        # Create some test files and check if they exist
        file_1 = tempfile.NamedTemporaryFile(delete=False, dir=self.test_dir)
        file_2 = tempfile.NamedTemporaryFile(delete=False, dir=self.test_dir)
        file_1.close()
        file_2.close()
        self.assertTrue(os.path.exists(file_1.name))
        self.assertTrue(os.path.exists(file_2.name))

        # Call the tested function
        clear_directory(self.test_dir)

        # Check if the tested directory is empty
        self.assertEqual(os.listdir(self.test_dir), [])

if __name__ == '__main__':
    unittest.main()
