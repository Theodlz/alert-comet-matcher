import json
import os
import pandas as pd

from pathlib import Path
from tqdm import tqdm
from src.config import load_config
from src.utils.paths import (
    comet_alerts_file,
    comet_alerts_folder,
    comet_positions_folder,
)

cfg = load_config()


def is_epoch_processed(epoch, processed_epochs):
    return processed_epochs["start"] <= epoch <= processed_epochs["end"]


def load_fetched_comets(verbose):
    if not os.path.exists(comet_positions_folder()):
        os.makedirs(comet_positions_folder(), exist_ok=True)

    parquet_files = list(Path(comet_positions_folder()).glob("*.parquet"))

    # Extract comet name by removing the last three parts of the filename
    comets = {
        "_".join(os.path.basename(file).split("_")[:-3]): file for file in parquet_files
    }
    if not os.path.exists(comet_alerts_folder()):
        os.makedirs(comet_alerts_folder(), exist_ok=True)

    # Get the epoch range from the first file (assuming all files have the same range)
    epochs = pd.read_parquet(next(iter(comets.values())))["times"]
    first_epoch, last_epoch = epochs.min(), epochs.max()

    # Filter out comets that have already been fully processed
    comets_to_process = {}
    for comet_name, file_path in comets.items():
        if os.path.exists(comet_alerts_file(comet_name)):
            with open(comet_alerts_file(comet_name), "r", encoding="utf-8") as f:
                data = json.load(f)
            processed_range = data.get("processed_epochs")
            if is_epoch_processed(first_epoch, processed_range) and is_epoch_processed(
                last_epoch, processed_range
            ):
                continue
        comets_to_process[comet_name] = comets[comet_name]

    print(f"Found {len(comets) - len(comets_to_process)} comets fully processed")

    if not comets_to_process:
        print("No comets to process, exiting")
        return

    print(f"Found {len(comets_to_process)} comets to process")

    # open the parquet files with pandas
    comets_with_positions = {}
    for comet_name in tqdm(
        comets_to_process, desc="Reading parquet files", disable=not verbose
    ):
        data = pd.read_parquet(comets_to_process[comet_name])
        comets_with_positions[comet_name] = {
            "ra": data["ra"].values,
            "dec": data["dec"].values,
            "jd": data["jd"].values if "jd" in data.columns else data["times"].values,
        }

    return comets_with_positions
