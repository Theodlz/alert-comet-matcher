import ray
import time

from src.generate_comet_positions.generate_comet_positions import (
    generate_comet_positions,
)
from src.utils.comets import fetch_comets
from src.config import load_config


@ray.remote
class CometPositionsGenerator:
    @staticmethod
    def generate_comet_positions_remote(
        comet_name, start_date, end_date, verbose, time_step="10m"
    ):
        try:
            return generate_comet_positions(
                comet_name, start_date, end_date, time_step, verbose
            )
        except Exception as e:
            print(f"Error fetching data for {comet_name}: {str(e)}")


def generate_all_comet_positions(start_date, end_date, verbose=True, n_workers=2):
    actor_pool = ray.util.ActorPool(
        [CometPositionsGenerator.remote() for _ in range(n_workers)]
    )
    comet_names = fetch_comets()

    for comet_name in comet_names:
        actor_pool.submit(
            lambda a, name: a.generate_comet_positions_remote.remote(
                name, start_date, end_date, verbose
            ),
            comet_name,
        )

    while actor_pool.has_next():
        actor_pool.get_next_unordered()
        time.sleep(1)


ray.init()
try:
    cfg = load_config()
    generate_all_comet_positions(
        start_date="2017-11-07",  # Date to start generating comet positions from (YYYY-MM-DD)
        end_date="2026-11-07",  # Date to stop generating comet positions (YYYY-MM-DD)
    )
    print("All comet positions generated successfully.")
finally:
    ray.shutdown()
