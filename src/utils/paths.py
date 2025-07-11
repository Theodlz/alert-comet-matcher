import os

from src.config import load_config

cfg = load_config()
data_path = cfg["params"]["data_path"]

def join_paths(*paths):
    return os.path.join(data_path, "comet_data", *paths)

def comet_alerts_folder():
    return join_paths("comet_alerts")

def comet_alerts_file(comet_name):
    return join_paths("comet_alerts", f"{comet_name}.json")

def comet_positions_folder():
    return join_paths("comet_positions")

def comet_positions_file(file_name):
    return join_paths("comet_positions", file_name)