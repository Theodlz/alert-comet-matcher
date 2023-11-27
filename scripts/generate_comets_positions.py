import ray
import time

from utils.comets import get_comet_data, get_comets_list

from config import load_config


@ray.remote
class CometDataFetcher:
    def __init__(self):
        pass

    def get_comet_data_remote(
        self,
        comet_name,
        start_date="2020-01-01",
        end_date="2020-01-02",
        time_step="10m",
        data_path="./data",
        verbose=True,
    ):
        return get_comet_data(
            comet_name, start_date, end_date, time_step, data_path, verbose
        )


def fetch_comets_data(
    start_date="2017-11-07",
    end_date="2023-11-07",
    source="yfernandez",
    data_path="./data",
    verbose=True,
):
    actor_pool = ray.util.ActorPool([CometDataFetcher.remote() for _ in range(2)])
    comet_names = get_comets_list(source)

    for comet_name in comet_names:
        actor_pool.submit(
            lambda a, comet_name: a.get_comet_data_remote.remote(
                comet_name, start_date, end_date, "10m", data_path, verbose
            ),
            comet_name,
        )

    while actor_pool.has_next():
        actor_pool.get_next_unordered()
        time.sleep(1)


ray.init()
config = load_config()
fetch_comets_data(data_path=config["ray"]["data"]["path"], verbose=True)
