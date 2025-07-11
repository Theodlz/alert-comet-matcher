import json
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import re
import os
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from src.config import load_config
from src.utils.alerts import bulk_query_moving_objects
from src.utils.kowalski import get_kowalski
from src.utils.moving_objects import get_object_positions
from src.utils.paths import (
    comet_alerts_file,
    comet_alerts_folder,
    comet_positions_folder,
    comet_positions_file
)

cfg = load_config()


def get_comets_list():
    r = requests.get(cfg["params"]["comet_list_url"])
    soup = BeautifulSoup(r.text, "html.parser")

    comet_pattern = re.compile(r"[A-Z]/\d+ [A-Z]+\d*")
    comets = []

    for pre in soup.find_all("pre"):
        lines = pre.text.splitlines()[4:]  # skip header lines
        for line in lines:
            match = comet_pattern.search(line)
            if match:
                comets.append(match.group())
    return comets


def update_alert_comets(
    k,
    n_processes,
    max_queries_per_batch,
    verbose,
):

    if k is None:
        k = get_kowalski(verbose=verbose)

    if not os.path.exists(comet_positions_folder()):
        os.makedirs(comet_positions_folder(), exist_ok=True)

    parquet_files = list(Path(comet_positions_folder()).glob("*.parquet"))

    # Extract comets name by removing the last three parts of the filename
    comets = { "_".join(os.path.basename(file).split("_")[:-3]): file for file in parquet_files }
    if not os.path.exists(comet_alerts_folder()):
        os.makedirs(comet_alerts_folder(), exist_ok=True)

    # Get the epochs range from the first file (assuming all files have the same range)
    epochs = pd.read_parquet(next(iter(comets.values())))["times"]
    first_epoch, last_epoch = epochs.min(), epochs.max()

    # Filter out comets that have already been fully processed
    comets_to_process = {}
    for comet_name, file_path in comets.items():
        if os.path.exists(comet_alerts_file(comet_name)):
            with open(comet_alerts_file(comet_name), "r", encoding="utf-8") as f:
                data = json.load(f)
            processed_range = data.get("processed_epochs")
            if is_epoch_processed(first_epoch, processed_range) and is_epoch_processed(last_epoch, processed_range):
                continue
        comets_to_process[comet_name] = comets[comet_name]

    print(f"Found {len(comets) - len(comets_to_process)} comets fully processed")

    if not comets_to_process:
        print("No comets to process, exiting")
        return

    print(f"Found {len(comets_to_process)} comets to process")

    # open the parquet files with pandas
    comet_positions = {}
    for comet_name in tqdm(comets_to_process, desc="Reading parquet files", disable=not verbose):
        data = pd.read_parquet(comets_to_process[comet_name])
        comet_positions[comet_name] = {
            "ra": data["ra"].values,
            "dec": data["dec"].values,
            "jd": data["jd"].values if "jd" in data.columns else data["times"].values,
        }

    bulk_query_moving_objects(
        k=k,
        objects_with_positions=comet_positions,
        n_processes=n_processes,
        max_queries_per_batch=max_queries_per_batch,
        verbose=verbose,
    )


def get_comet_data(comet_name: str, start_date, end_date, time_step, verbose):
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

    data = get_object_positions(
        comet_name, start_date, end_date, time_step=time_step, verbose=verbose
    )
    # put that in a dataframe
    data = pd.DataFrame(data)
    data.to_parquet(
        comet_positions_file(file_name),
        index=False,
    )
