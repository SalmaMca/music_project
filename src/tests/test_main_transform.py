import os
import unittest
import pandas as pd
import sys

sys.path.append(".")
sys.path.append("..")
from src.music_project.main_pandas import main_transform as mt_pandas
from src.music_project.main_polars import main_transform as mt_polars


class MainTransformTestCase(unittest.TestCase):
    def setUp(self):
        # Generate sample data
        df = pd.DataFrame(
            {
                "sng_id": ["Song A", "Song B", "Song C", "Song D", "Song E", "Song F"],
                "user_id": ["user1", "user2", "user3", "user4", "user5", "user6"],
                "country": ["US", "US", "UK", "UK", "CA", "CA"],
            }
        )

        # Create a temporary output folder
        self.output_folder = "test_output"
        os.makedirs(self.output_folder, exist_ok=True)
        self.data_name="sample_data-20230101.log"
        self.user_data_name="users_top50_20230101.csv"
        # Save the sample data as CSV
        df.to_csv(
            f"./{self.output_folder}/{self.data_name}", index=False, header=False, sep="|"
        )

    def tearDown(self):
        # Clean up the temporary files and folders
        os.remove(f"./{self.output_folder}/{self.data_name}")

    def test_main_transform_pandas(self):
        file_name = f"./{self.output_folder}/{self.data_name}"

        # Call the function under test
        mt_pandas(file_name, self.output_folder)

    
        # Assertions
        self.assertTrue(
            os.path.exists(f"{self.output_folder}/{self.user_data_name}")
        )

    def test_main_transform_polars(self):
        file_name = f"./{self.output_folder}/{self.data_name}"

        # Call the function under test
        mt_polars(file_name, self.output_folder)

        # Assertions
        self.assertTrue(
            os.path.exists(f"{self.output_folder}/{self.user_data_name}")
        )


if __name__ == "__main__":
    unittest.main()
