import requests
from bs4 import BeautifulSoup
import re
import os
import glob
import joblib
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from src.utils.alerts import bulk_query_moving_objects
from src.utils.kowalski import get_kowalski
from src.utils.moving_objects import get_object_positions

SOURCES = {"yfernandez": "https://physics.ucf.edu/~yfernandez/cometlist.html"}


def get_comets_list(source="yfernandez"):
    # verify that the source is valid
    if source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES.keys()}")

    comets = []
    if source == "yfernandez":
        # get the list of comets from the website
        # https://physics.ucf.edu/~yfernandez/cometlist.html
        # parse the html
        # return a list of comets
        r = requests.get(SOURCES[source])
        soup = BeautifulSoup(r.text, "html.parser")
        pre_tags = soup.find_all("pre")
        options = [r"([A-Z]\/\d+ [A-Z]+\d+)", r"([A-Z]\/\d+ [A-Z]+\d*)"]
        for pre_tag in pre_tags:
            for line in pre_tag.text.split("\n")[4:]:
                for option in options:
                    match = re.search(option, line)
                    if match:
                        comets.append(match.group(0))
                        break
    else:
        raise NotImplementedError(f"source {source} not implemented yet!")
    return comets


def update_alert_comets(
    k,
    data_path,
    alert_stream="ztf",
    n_processes=20,
    max_queries_per_batch=None,
    verbose=False,
    update_alert_packets=False,
):

    if k is None:
        k = get_kowalski(verbose=verbose)
    # data path should have a comet_data dir, with a comet_csv dir
    # create the comet_data and comet_csv dirs if they don't exist
    if not os.path.exists(data_path):
        os.mkdir(data_path)
    comet_data_path = os.path.join(data_path, "comet_data")
    if not os.path.exists(comet_data_path):
        os.mkdir(comet_data_path)
    comet_positions_path = os.path.join(comet_data_path, "positions")
    if not os.path.exists(comet_positions_path):
        os.mkdir(comet_positions_path)

    # look for the csv files in this directory
    csv_files = []
    for file in glob.glob(os.path.join(comet_positions_path, "*.csv")):
        if file.endswith(".csv"):
            csv_files.append(file)

    # from the file names, remove the start and end dates in the format YYYYMMDD, to extract the comet names
    comet_names = {}
    for file in csv_files:
        split_name = file.split("/")[-1].split("_")
        # get everything before the second to last element
        comet_name = "_".join(split_name[:-3])
        comet_names[comet_name] = file

    # in the data path, we should also have a joblib file with the alerts that have already been fetched for each comet
    try:
        with open(os.path.join(comet_data_path, "alert_comets.joblib"), "rb") as f:
            existing_comet_alerts = joblib.load(f)
    except FileNotFoundError:
        print("No existing alerts found")
        existing_comet_alerts = {}

    print(f"Found {len(existing_comet_alerts)} comets with existing alerts")

    # remove from comet_names the ones that already have an entry in existing_comet_alerts
    for comet_name in existing_comet_alerts:
        if comet_name in comet_names:
            del comet_names[comet_name]

    print(f"Found {len(comet_names)} comets to query for")

    if len(comet_names) == 0:
        if not update_alert_packets:
            print("No comets to query for, exiting")
            return
        else:
            print("No comets to query for, updating alert packets")
            # here we will simply loop over the existing_comet_alerts and update the alert packets
            for comet_name in tqdm(
                existing_comet_alerts,
                desc="Updating alert packets",
                disable=not verbose,
            ):
                # get the alert packets for this comet
                for i in range(len(existing_comet_alerts[comet_name])):
                    query = {
                        "query_type": "find",
                        "query": {
                            "catalog": "ZTF_alerts",
                            "filter": {
                                "candid": existing_comet_alerts[comet_name][i]["candid"]
                            },
                            "projection": {
                                "_id": 0,
                                "candid": 1,
                                "objectId": 1,
                                "candidate": 1,
                                "cutoutScience": 1,
                                "cutoutTemplate": 1,
                                "cutoutDifference": 1,
                                "classifications": 1,
                            },
                        },
                    }
                    results = k.query(query=query)  # noqa F841
                    raise NotImplementedError("Need to update the alert packets")

            # save the new alerts
            with open(os.path.join(comet_data_path, "alert_comets.joblib"), "wb") as f:
                joblib.dump(existing_comet_alerts, f)

    # open the csv files with pandas
    comet_positions = {}
    for comet_name in tqdm(comet_names, desc="Reading csv files", disable=not verbose):
        data = pd.read_csv(comet_names[comet_name])
        comet_positions[comet_name] = {
            "ra": data["ra"].values,
            "dec": data["dec"].values,
            "jd": data["jd"].values if "jd" in data.columns else data["times"].values,
        }

    comet_alerts = bulk_query_moving_objects(
        k=k,
        objects_with_positions=comet_positions,
        alert_stream=alert_stream,
        n_processes=n_processes,
        max_queries_per_batch=max_queries_per_batch,
        verbose=verbose,
    )

    # add the new alerts to existing_comet_alerts
    existing_comet_alerts = {**existing_comet_alerts, **comet_alerts}

    # save the new alerts
    with open(os.path.join(comet_data_path, "alert_comets.joblib"), "wb") as f:
        joblib.dump(existing_comet_alerts, f)


def get_comet_data(
    comet_name: str,
    start_date,
    end_date,
    time_step="10m",
    data_path="./data",
    verbose=False,
):
    # save the dataframe to a csv file in the data_path + positions dir
    comet_data_path = os.path.join(data_path, "comet_data")
    comet_positions_path = os.path.join(comet_data_path, "positions")
    if not os.path.exists(comet_positions_path):
        os.mkdir(comet_positions_path)

    # conver to format YYMMDD
    start_date_str = datetime.strptime(start_date, "%Y-%m-%d").strftime("%y%m%d")
    end_date_str = datetime.strptime(end_date, "%Y-%m-%d").strftime("%y%m%d")

    file_name = f'{comet_name.replace("/", "_").replace(" ", "_")}_{start_date_str}_{end_date_str}_{time_step}.csv'
    # if the file already exists, skip
    if os.path.exists(os.path.join(comet_positions_path, file_name)):
        print(f"File {file_name} already exists, skipping")
        return

    data = get_object_positions(
        comet_name, start_date, end_date, time_step=time_step, verbose=verbose
    )
    # put that in a dataframe
    data = pd.DataFrame(data)
    data.to_csv(
        os.path.join(comet_positions_path, file_name),
        index=False,
    )
