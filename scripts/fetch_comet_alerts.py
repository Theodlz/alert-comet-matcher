import ray
import time

from utils.comets import update_alert_comets
from config import load_config

config = load_config()


@ray.remote
def update_alert_comets_remote(
    k,
    data_path="./data",
    alert_stream="ztf",
    n_processes=20,
    max_queries_per_batch=None,
    verbose=True,
    loop=True,
):
    if loop:
        print("Starting update loop")
        while True:
            update_alert_comets(
                k, data_path, alert_stream, n_processes, max_queries_per_batch, verbose
            )
            time.sleep(60)
    else:
        update_alert_comets(
            k, data_path, alert_stream, n_processes, max_queries_per_batch, verbose
        )


ray.init()
alert_stream = "ztf"
n_processes = 20
max_queries_per_batch = config["params"]["max_queries_per_batch"]
loop = True
result = update_alert_comets_remote.remote(
    None,
    config["ray"]["data"]["path"],
    alert_stream,
    n_processes,
    max_queries_per_batch=None,
    verbose=True,
    loop=False,
)
ray.get(result)
