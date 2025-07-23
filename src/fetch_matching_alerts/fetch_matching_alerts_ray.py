import ray
import time

from src.utils.kowalski import get_kowalski
from src.fetch_matching_alerts.fetch_matching_alerts import fetch_comet_matching_alerts


@ray.remote
def fetch_comet_matching_alerts_remote(
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
                verbose,
            )
            time.sleep(60)
    else:
        fetch_comet_matching_alerts(
            kowalski,
            n_processes,
            verbose,
        )


ray.init()
try:
    ray.get(fetch_comet_matching_alerts_remote.remote(loop=True))
finally:
    ray.shutdown()
