import os

from src.config import load_config

cfg = load_config()
data_path = cfg["params"]["data_path"]

def comet_alerts_folder():
    return os.path.join("comet_data", "comet_alerts")

def comet_alerts_file(comet_name):
    return os.path.join("comet_data", "comet_alerts", f"{comet_name}.json")

def comet_positions_folder():
    return os.path.join("comet_data", "comet_positions")

def comet_positions_file(file_name):
    return os.path.join("comet_data", "comet_positions", f"{file_name}")