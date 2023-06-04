import os
import unittest
import pandas as pd
import sys

sys.path.append(".")
sys.path.append("..")
from music_project.main_pandas import main_transform as mt_pandas
from music_project.main_polars import main_transform as mt_polars


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
        self.output_folder = "output"
        os.makedirs(self.output_folder, exist_ok=True)

        # Save the sample data as CSV
        df.to_csv(
            "./output/sample_data_20230101.log", index=False, header=False, sep="|"
        )

    def tearDown(self):
        # Clean up the temporary files and folders
        os.remove("./output/sample_data_20230101.log")

    def test_main_transform_pandas(self):
        file_name = "./output/sample_data_20230101.log"

        # Call the function under test
        mt_pandas(file_name, self.output_folder)

        # Assertions
        self.assertTrue(
            os.path.exists(f"{self.output_folder}/users_top50_20230101.csv")
        )

    def test_main_transform_polars(self):
        file_name = "./output/sample_data_20230101.log"

        # Call the function under test
        mt_polars(file_name, self.output_folder)

        # Assertions
        self.assertTrue(
            os.path.exists(f"{self.output_folder}/users_top50_20230101.csv")
        )


if __name__ == "__main__":
    unittest.main()
