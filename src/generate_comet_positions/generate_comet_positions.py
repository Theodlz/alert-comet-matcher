import os
import pandas as pd

from datetime import datetime

from src.utils.comets import get_comet_positions
from src.utils.paths import comet_positions_folder, comet_positions_file


def generate_comet_positions(comet_name: str, start_date, end_date, time_step, verbose):
    # save the dataframe to a parquet file
    if not os.path.exists(comet_positions_folder()):
        os.makedirs(comet_positions_folder(), exist_ok=True)

    # convert to format YYMMDD
    start_date_str = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    end_date_str = datetime.strptime(end_date, "%Y-%m-%d").strftime("%y%m%d")

    file_name = f'{comet_name.replace("/", "_").replace(" ", "_")}_{start_date_str}_{end_date_str}_{time_step}'
    if os.path.exists(comet_positions_file(file_name)):
        print(f"File {file_name} already exists, skipping")
        return

    positions = get_comet_positions(
        comet_name, start_date, end_date, time_step, verbose=verbose
    )
    if not positions:
        print(f"No data for {comet_name}, skipping")
        return

    # Put the data into a DataFrame and save it as a parquet file
    pd.DataFrame(positions).to_parquet(
        comet_positions_file(file_name),
        index=False,
    )
