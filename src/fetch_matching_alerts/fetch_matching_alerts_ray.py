import ray
import time

from src.config import load_config
from src.utils.kowalski import get_kowalski
from src.fetch_matching_alerts.fetch_matching_alerts import fetch_comet_matching_alerts


@ray.remote
def fetch_comet_matching_alerts_remote(
    max_queries_per_batch,
    n_processes=12,
    verbose=True,
    loop=False,
):
    kowalski = get_kowalski(verbose=verbose)
    if loop:
        print("Starting alert fetch loop for comet position matching...")
        while True:
            fetch_comet_matching_alerts(
                kowalski,
                n_processes,
                max_queries_per_batch,
                verbose,
            )
            time.sleep(60)
    else:
        fetch_comet_matching_alerts(
            kowalski,
            n_processes,
            max_queries_per_batch,
            verbose,
        )


ray.init()
try:
    cfg = load_config()
    ray.get(
        fetch_comet_matching_alerts_remote.remote(
            cfg["params.max_queries_per_batch"],
            loop=True,
        )
    )
finally:
    ray.shutdown()
