import requests
from bs4 import BeautifulSoup
import re
import os
import glob
import joblib
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from src.config import load_config
from src.utils.alerts import bulk_query_moving_objects
from src.utils.kowalski import get_kowalski
from src.utils.moving_objects import get_object_positions

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
    data_path,
    n_processes,
    max_queries_per_batch,
    verbose,
):

    if k is None:
        k = get_kowalski(verbose=verbose)
    # data path should have a comet_data dir, with a comet_parquet dir
    # create the comet_data and comet_parquet dirs if they don't exist
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    comet_data_path = os.path.join(data_path, "comet_data")
    if not os.path.exists(comet_data_path):
        os.mkdir(comet_data_path)
    comet_positions_path = os.path.join(comet_data_path, "positions")
    if not os.path.exists(comet_positions_path):
        os.mkdir(comet_positions_path)

    # look for the parquet files in this directory
    parquet_files = []
    for file in glob.glob(os.path.join(comet_positions_path, "*.parquet")):
        if file.endswith(".parquet"):
            parquet_files.append(file)

    # Extract comets name by removing the last three parts of the filename
    comet_names = {}
    for file in parquet_files:
        split_name = file.split("/")[-1].split("_")
        # get everything before the second to last element
        comet_name = "_".join(split_name[:-3])
        comet_names[comet_name] = file

    comet_names2 = {
        "_".join(os.path.basename(file).split("_")[:-3]): file
        for file in parquet_files
    }
    alerts_path = os.path.join(comet_data_path, "alert_comets.joblib")

    # Load existing alerts if available
    if os.path.exists(alerts_path):
        existing_comet_alerts = joblib.load(open(alerts_path, "rb"))
        print(f"Found {len(existing_comet_alerts)} comets with existing alerts")
    else:
        existing_comet_alerts = {}

    # remove from comet_names the ones that already have an entry in existing_comet_alerts
    comet_names = {
        name: path for name, path in comet_names.items()
        if name not in existing_comet_alerts
    }

    if not comet_names:
        print("No comets to query for, exiting")
        return

    print(f"Found {len(comet_names)} comets to query for")

    # open the parquet files with pandas
    comet_positions = {}
    for comet_name in tqdm(comet_names, desc="Reading parquet files", disable=not verbose):
        data = pd.read_parquet(comet_names[comet_name])
        comet_positions[comet_name] = {
            "ra": data["ra"].values,
            "dec": data["dec"].values,
            "jd": data["jd"].values if "jd" in data.columns else data["times"].values,
        }

    comet_alerts = bulk_query_moving_objects(
        k=k,
        objects_with_positions=comet_positions,
        n_processes=n_processes,
        max_queries_per_batch=max_queries_per_batch,
        verbose=verbose,
    )

    # add the new alerts to existing_comet_alerts
    existing_comet_alerts = {**existing_comet_alerts, **comet_alerts}
    joblib.dump(existing_comet_alerts, open(alerts_path, "wb"))


def get_comet_data(comet_name: str, start_date, end_date, time_step, data_path, verbose):
    # save the dataframe to a parquet file in the data_path + positions dir
    comet_data_path = os.path.join(data_path, "comet_data")
    comet_positions_path = os.path.join(comet_data_path, "positions")
    if not os.path.exists(comet_positions_path):
        os.makedirs(comet_positions_path, exist_ok=True)

    # convert to format YYMMDD
    start_date_str = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    end_date_str = datetime.strptime(end_date, "%Y-%m-%d").strftime("%y%m%d")

    file_name = f'{comet_name.replace("/", "_").replace(" ", "_")}_{start_date_str}_{end_date_str}_{time_step}.parquet'
    # if the file already exists, skip
    if os.path.exists(os.path.join(comet_positions_path, file_name)):
        print(f"File {file_name} already exists, skipping")
        return

    data = get_object_positions(
        comet_name, start_date, end_date, time_step=time_step, verbose=verbose
    )
    # put that in a dataframe
    data = pd.DataFrame(data)
    data.to_parquet(
        os.path.join(comet_positions_path, file_name),
        index=False,
    )
