import ray
import time

from src.utils.comets import get_comet_data, get_comets_list
from src.config import load_config


@ray.remote
class CometDataFetcher:
    def __init__(self):
        pass

    def get_comet_data_remote(self, comet_name, start_date, end_date, data_path, verbose, time_step="10m"):
        try:
            return get_comet_data(
                comet_name, start_date, end_date, time_step, data_path, verbose
            )
        except Exception as e:
            print(f"Error fetching data for {comet_name}: {str(e)}")


def fetch_comets_data(start_date, end_date, data_path, verbose=True):
    actor_pool = ray.util.ActorPool([CometDataFetcher.remote() for _ in range(2)])
    comet_names = get_comets_list()

    for comet_name in comet_names:
        actor_pool.submit(
            lambda a, comet_name: a.get_comet_data_remote.remote(
                comet_name, start_date, end_date, data_path, verbose
            ),
            comet_name,
        )

    while actor_pool.has_next():
        actor_pool.get_next_unordered()
        time.sleep(1)


ray.init()
cfg = load_config()
fetch_comets_data(
    start_date="2017-11-07",
    end_date="2023-11-07",
    data_path=cfg["ray"]["data"]["path"],
)
