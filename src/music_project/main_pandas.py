import os

import pandas as pd


def daily_file(file_name, file_delimiter, col_names_type):
    """
    Read a daily file and return a pandas DataFrame.

    Parameters:
        file_name (str): The path to the file to be read.
        file_delimiter (str): The delimiter used in the file.
        col_names_type (dict): A dict of column names and data types to be used for the DataFrame.

    Returns:
        pandas.DataFrame: The DataFrame containing the data read from the file.
    """

    # Read the file into a DataFrame using pd.read_table() function
    # file_name: Path to the file
    # delimiter: Delimiter used in the file to separate values
    # on_bad_lines: Specifies how to handle problematic lines in the file (here, "warn" shows a warning)
    # names: List of column names to be used for the DataFrame

    df = pd.read_table(
        file_name,
        delimiter=file_delimiter,
        on_bad_lines="warn",
        names=col_names_type.keys(),
        dtype=col_names_type,
        keep_default_na=False,
        na_values=["NaN", "null", "nan", "Nan", "NA", ""],
    )

    # Return the DataFrame containing the data read from the file
    return df


def data_quality_check(df):
    """
    Perform a data quality check on a DataFrame.

    Parameters:
        df (pandas.DataFrame): The DataFrame to be checked.

    Returns:
        pandas.DataFrame: The DataFrame with rows filtered based on the data quality check.
    """

    # Filter the DataFrame based on the length of the 'country' column values
    # Keep only rows where the length of the 'country' values is equal to 2
    df = df[df["country"].str.len() == 2]


    # drop nulls
    df = df.dropna()

    # Return the filtered DataFrame
    return df


def extract_date_from_path(file_name):#cette fonction a pour but extraire la date pour nommer output souhaite
    """
    Extract the date from the file path.

    Parameters:
        list_file_name (list): A list of file paths.

    Returns:
        str: The extracted date from the file path.
    """

    # Get the latest file path from the sorted list
   

    # Extract the file name from the file path
    file_name = os.path.basename(file_name)

    # Remove the file extension by splitting the file name and taking the first part
    no_ext_file = file_name.split(".")[0]

    # Extract the date from the file name by splitting it based on underscores and taking the last part
    final_date = no_ext_file.split("-")[-1]

    # Return the extracted date
    return final_date


def get_df(file_name):
    """
    Read and concatenate daily files, perform data quality check, and extract the date from the file path.

    Parameters:
        list_file_name (list): A list of file paths.

    Returns:
        tuple: A tuple containing the concatenated DataFrame and the extracted date from the file path.
    """

    # Create an empty list to store the DataFrame

    # Iterate over each file name in the list
    
        # Read the daily file into a DataFrame and append it to the list
    df=daily_file(
                file_name,
                "|",
                col_names_type={"sng_id": object, "user_id": object, "country": object},
            )
        

    # Concatenate the list of DataFrames into a single DataFrame


    # Perform a data quality check on the DataFrame
    df = data_quality_check(df)

    # Extract the date from the file path
    final_date = extract_date_from_path(file_name)

    # Return the concatenated DataFrame and the extracted date
    return df, final_date


def aggregate_transform(df, col_name, best_of):
    """
    Aggregate and transform a DataFrame based on specified columns.

    Parameters:
        df (pandas.DataFrame): The DataFrame to be transformed.
        col_name (str): The  column to group by.
        best_of (int): The number of top values to select after grouping.

    Returns:
        pandas.DataFrame: The transformed DataFrame.
    """

    return (
        df.groupby(["sng_id", col_name])
        .agg(stream_count=(col_name, "count"))
        .groupby(col_name, group_keys=False)["stream_count"]
        .apply(lambda x: x.nlargest(best_of))
        .reset_index()
    )
    # not safe wasn't sure how the index are kept between the groupby and the head
    # gives the exact same result as above
    # need more test
    # return (
    #     df.groupby(["sng_id", first_col])
    #     .agg(stream_count=(first_col, "count"))
    #     # sort values largest to smallest
    #     .sort_values([first_col, "stream_count"], ascending=[True, False])
    #     # group by preserve the order of rows in each group
    #     .groupby(first_col)
    #     # when using head thge original index is preserved
    #     .head(best_of)
    #     # get back the column that got implicitly turn to index by the groupby
    #     .reset_index()
    # )


def save_file(top_50, output_folder, grouping_col, date):
    """
    Save the top 50 data to individual files based on a grouping column.

    Parameters:
        top_50 (pandas.DataFrame): The DataFrame containing the top 50 data.
        output_folder (str): The path to the output folder.
        grouping_col (str): The column to group the data by.
        date (str): The date to include in the file names.

    Returns:
        None
    """

    # Group the top_50 DataFrame based on the grouping column wich can be country or user
    groups = top_50.groupby(grouping_col)

    # Iterate over each group
    for name, group in groups:
        # Construct the file path using the output folder, group name, and date
        file_path = os.path.join(output_folder, f"{name}_top50_{date}.txt")

        # Open the file for writing
        with open(file_path, "w") as file:
            # Generate the data to be written to the file
            data = f"{name}|" + ",".join(
                [
                    f"{x}:{i+1}"
                    for i, x in enumerate(
                        group.sort_values("stream_count", ascending=False)[
                            "sng_id"
                        ].tolist()
                    )
                ]
            )

            # Write the data to the file
            file.write(data)


def main_transform(file_name, output_folder):
    """
    Perform the main transformation process on a list of files and save the results.

    Parameters:
        list_file_name (list): A list of file paths.
        output_folder (str): The path to the output folder.

    Returns:
        None
    """

    # Perform daily file processing and extract the date
    df, date = get_df(file_name)

    # Aggregate and transform the data by country, selecting the top 50 songs
    top_50_songs_by_country = aggregate_transform(df, col_name="country", best_of=50)

    # Save the top 50 songs by country to individual files based on country
    save_file(top_50_songs_by_country, output_folder, grouping_col="country", date=date)

    # Aggregate and transform the data by user_id, selecting the top 50 songs
    top_50_songs_by_user = aggregate_transform(df, col_name="user_id", best_of=50)

    # Save the top 50 songs by user_id to a CSV file
    top_50_songs_by_user.to_csv(
        os.path.join(output_folder, f"users_top50_{date}.csv"), index=None
    )
