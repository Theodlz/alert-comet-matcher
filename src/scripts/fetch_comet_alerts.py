import ray
import time

from src.utils.comets import update_alert_comets
from src.config import load_config

config = load_config()


@ray.remote
def update_alert_comets_remote(
    k,
    data_path="./data",
    alert_stream="ztf",
    n_processes=20,
    max_queries_per_batch=None,
    update_alert_packets=False,
    verbose=True,
    loop=True,
):
    if loop:
        print("Starting update loop")
        while True:
            update_alert_comets(
                k,
                data_path,
                alert_stream,
                n_processes,
                max_queries_per_batch,
                verbose,
                update_alert_packets=update_alert_packets,
            )
            time.sleep(60)
    else:
        update_alert_comets(
            k,
            data_path,
            alert_stream,
            n_processes,
            max_queries_per_batch,
            verbose,
            update_alert_packets=update_alert_packets,
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
    max_queries_per_batch=max_queries_per_batch,
    update_alert_packets=True,
    verbose=True,
    loop=False,
)
ray.get(result)
