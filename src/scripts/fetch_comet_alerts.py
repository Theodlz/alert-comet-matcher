import ray
import time

from src.utils.comets import update_alert_comets
from src.config import load_config

cfg = load_config()


@ray.remote
def update_alert_comets_remote(
    k,
    max_queries_per_batch,
    n_processes=12,
    verbose=True,
    loop=False,
):
    if loop:
        print("Starting update loop")
        while True:
            update_alert_comets(
                k,
                n_processes,
                max_queries_per_batch,
                verbose,
            )
            time.sleep(60)
    else:
        update_alert_comets(
            k,
            n_processes,
            max_queries_per_batch,
            verbose,
        )


ray.init()
ray.get(update_alert_comets_remote.remote(
    None,
    cfg["params"]["max_queries_per_batch"],
    loop=True
))