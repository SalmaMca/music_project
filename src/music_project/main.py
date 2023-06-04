import argparse

import main_pandas
import main_polars


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file_name",
        help="enter a list of full paths seperated by a space",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--output_folder",
        help="enter the output folder where the results will be saved (full path)",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--engine",
        help="enter engine <Polaris> or <Pandas>",
        type=str,
        choices=["Pandas", "Polars"],
        required=True,
    )
    args = parser.parse_args()
    assert args.file_name is not None, "file_name must be a given"
    assert args.output_folder is not None, "output_folder must be a given"
    if args.engine == "Pandas":
        main_pandas.main_transform(args.file_name, args.output_folder)
    elif args.engine == "Polars":
        import polars as pl
        # super low memory usage
        import os
        os.environ["POLARS_MAX_THREADS"] = "1"
        pl.Config.set_streaming_chunk_size(100)
        #########################################
        main_polars.main_transform(args.file_name, args.output_folder)
    else:
        raise ValueError(f"engine {args.engine} not recognized")


if __name__ == "__main__":
    main()
