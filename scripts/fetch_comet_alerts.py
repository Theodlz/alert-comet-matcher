import ray
import time

from utils.comets import update_alert_comets
from config import load_config

config = load_config()


@ray.remote
def update_alert_comets_remote(
    k, data_path="./data", alert_stream="ztf", n_processes=20, verbose=True, loop=True
):
    if loop:
        print("Starting update loop")
        while True:
            update_alert_comets(k, data_path, alert_stream, n_processes, verbose)
            time.sleep(60)
    else:
        update_alert_comets(k, data_path, alert_stream, n_processes, verbose)


ray.init()
alert_stream = "ztf"
n_processes = 20
loop = True
result = update_alert_comets_remote.remote(
    None,
    config["ray"]["data"]["path"],
    alert_stream,
    n_processes,
    verbose=True,
    loop=False,
)
ray.get(result)
