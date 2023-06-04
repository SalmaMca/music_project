import os
import polars as pl


def extract_date_from_path(file_name):
    """
    Extract the date from the file path.

    Parameters:
        file_name : A file path.

    Returns:
        str: The extracted date from the file path.
    """
    # Get the latest file path from the sorted list
 
    # Extract the file name from the file path
    file_name = os.path.basename(file_name)
    # Remove the file extension by splitting the file name and taking the first part
    no_ext_file = file_name.split(".")[0]
    # Extract the date from the file name by splitting it based on '-' and taking the last part
    final_date = no_ext_file.split("-")[-1]
    # Return the extracted date
    return final_date


def reduce_save_udf(args):
    """
    Applies reduction and formatting to the input arguments and returns a formatted string.

    Parameters:
        args (tuple): A tuple containing two lists or iterables.

    Returns:
        pandas.Series: A pandas Series object containing a single element - the formatted string.

    """
    results_list = sorted(list(zip(args[0], args[1])), key=lambda x: x[1], reverse=True)
    formated_string = ",".join([f"{x[0]}:{i+1}" for i, x in enumerate(results_list)])
    return pl.Series([formated_string], dtype=pl.Utf8)


def lazy_load(file_name):
    """
    Load multiple CSV files lazily and concatenate the resulting lazy DataFrames.

    Args:
        file_name (list): A file name of the CSV file to load.

    Returns:
        pl.DataFrame: A lazy DataFrame obtained by concatenating the lazy DataFrames
                      loaded from the CSV files.

    """
    lazy_dataframes = pl.scan_csv(
                file_name,
                ignore_errors=True,
                separator="|",
                dtypes=[pl.Utf8, pl.Utf8, pl.Utf8],
                new_columns=["sng_id", "user_id", "country"],
                low_memory=True,
                has_header=False,
                null_values=["NaN", "null", "nan", "Nan", "NA", ""],
            )
  
    return lazy_dataframes


def stack_transforms(lazy_dataframe, col_name, best_of):
    """
    Applies a series of transformations to the input lazy dataframe.

    Args:
        lazy_dataframe: A lazy dataframe object.
        col_name: Name of the column to be used in the groupby operation.
        best_of: An integer specifying the number of top values to select in the final aggregation.

    Returns:
        A transformed dataframe with the following schema:
        {
            'sng_id': Utf8,
            'country': Utf8,
            'stream_count': UInt32
        }
    """
    return (
        lazy_dataframe.drop_nulls()
        .filter(pl.col("country").str.lengths() == 2)
        .groupby(["sng_id", col_name])
        .agg(pl.count().alias("stream_count"))
        .groupby(col_name)
        .apply(
            lambda x: x.top_k(best_of, by="stream_count"),
            schema={"sng_id": pl.Utf8, "country": pl.Utf8, "stream_count": pl.UInt32},
        )
    )


def lazy_format(lazy_dataframe, col_name):
    """
    Groups the 'lazy_dataframe' by the 'col_name' column while maintaining the order.
    Applies the 'reduce_save_udf' function to the 'sng_id' and 'stream_count' columns,
    and aliases the result as 'results'.

    Args:
        lazy_dataframe (pyspark.sql.DataFrame): The lazy dataframe to be grouped.
        col_name (str): The name of the column to group by.

    Returns:
        pyspark.sql.DataFrame: The resulting dataframe with the grouped and aggregated data.

    """
    return lazy_dataframe.groupby(col_name, maintain_order=True).agg(
        pl.apply(exprs=["sng_id", "stream_count"], function=reduce_save_udf).alias(
            "results"
        )
    )


def lazy_transform(file_name, col_name, best_of=50, show_graph=True):
    """
    Apply lazy transformation operations to a DataFrame loaded from a file.

    Args:
        file_name (str): The name of the file to load the DataFrame from.
        col_name (str): The name of the column to use for transformation.
        best_of (int, optional): The number of best records to select during transformation. Defaults to 50.
        show_graph (bool, optional): Flag indicating whether to display the execution graph. Defaults to True.

    Returns:
        tuple: A tuple containing the transformed DataFrame and the extracted date.

    Raises:
        FileNotFoundError: If the specified file do not exist.

    Examples:
        # Load a DataFrame from a single file and apply lazy transformations
        dataframe, date = lazy_transform("data.csv", "country")

    """
    lazy_dataframe = lazy_load(file_name)
    lazy_dataframe = stack_transforms(lazy_dataframe, col_name, best_of)

    if col_name == "country":
        lazy_dataframe = lazy_format(lazy_dataframe, col_name)

    if show_graph:
        lazy_dataframe.show_graph(output_path=f"./img/{col_name}_lazy_execution_graph.png")

    final_date = extract_date_from_path(file_name)

    return lazy_dataframe, final_date


def file_save(lazy_dataframe, output_folder, col_name, final_date):
    """
    Saves the data from a lazy dataframe to file(s) based on the specified criteria.

    Args:
        lazy_dataframe (LazyDataFrame): A lazy dataframe object.
        output_folder (str): The folder path where the output file(s) will be saved.
        col_name (str): The column name used for determining the file saving behavior. Valid values are "country" or "user_id".
        final_date (str): The final date used in the output file name.

    Returns:
        None

    Raises:
        None

    """
    lazy_dataframe = lazy_dataframe.collect(streaming=True)
    if col_name == "country":
        for row in lazy_dataframe.rows(named=True):
            file_path = os.path.join(
                output_folder, f"{row['country']}_top50_{final_date}.txt"
            )
            with open(file_path, "w") as file:
                file.write(f"{row['country']}|" + row["results"])
    elif col_name == "user_id":
        lazy_dataframe.write_csv(
            os.path.join(output_folder, f"users_top50_{final_date}.csv")
        )


def main_transform(file_name, output_folder):
    """
    Transforms the data from the given file name and saves the results to the specified output folder.

    Args:
        file_name : A  file name to process.
        output_folder (str): The path to the output folder where the transformed data will be saved.
        low_memory (bool, optional): A flag indicating whether to activate low memory mode. Defaults to True.

    Returns:
        None

    Raises:
        Any relevant exceptions that may occur during the execution of the function.

    """

    # Perform lazy transformation on country data
    lazy_dataframe, final_date = lazy_transform(
        file_name, col_name="country", best_of=50, show_graph=True
    )
    file_save(lazy_dataframe, output_folder, col_name="country", final_date=final_date)
    del lazy_dataframe
    # Perform lazy transformation on user_id data
    lazy_dataframe, final_date = lazy_transform(
       file_name, col_name="user_id", best_of=50, show_graph=True
    )
    file_save(lazy_dataframe, output_folder, col_name="user_id", final_date=final_date)
