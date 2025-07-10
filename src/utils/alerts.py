from tqdm import tqdm

from src.utils.kowalski import build_cone_search, run_queries, Kowalski
from src.config import load_config

cfg = load_config()

def bulk_query_moving_objects(
    k: Kowalski,
    objects_with_positions: dict,
    n_processes,
    max_queries_per_batch,
    verbose,
):
    stream = cfg["params"]["alert_stream"]
    if len(objects_with_positions) == 0:
        return {}
    # make sure the epochs are sorted for each object
    for obj_name in objects_with_positions:
        objects_with_positions[obj_name]["jd"] = sorted(
            objects_with_positions[obj_name]["jd"]
        )

    if len(objects_with_positions) > 1:
        # verify that all the objects have the same epochs
        # if not, raise an exception
        epochs = set()
        for obj_name in objects_with_positions:
            epochs.add(tuple(objects_with_positions[obj_name]["jd"]))
        if len(epochs) > 1:
            raise Exception("Objects have different epochs")

    print("Generating queries...")

    # reformat to have a dict with keys as epochs and values as lists of objects and their positions at that epoch
    epochs = tuple(objects_with_positions[list(objects_with_positions.keys())[0]]["jd"])
    if max_queries_per_batch is None:
        queries = []
        for i, epoch in tqdm(
            enumerate(epochs),
            total=len(epochs),
            desc="Generating queries",
            disable=not verbose,
        ):
            objects = {}
            catalog_parameters = {
                stream: {
                    "filter": {
                        "candidate.jd": {"$gte": epoch - 0.01, "$lte": epoch + 0.01}
                    },
                    "projection": {
                        "_id": 0,
                        "candid": 1,
                        "objectId": 1,
                        "candidate.jd": 1,
                    },
                }
            }
            for obj_name in objects_with_positions:
                objects[obj_name] = [
                    objects_with_positions[obj_name]["ra"][i],
                    objects_with_positions[obj_name]["dec"][i],
                ]
            queries.append(
                build_cone_search(
                    objects, catalog_parameters, radius=5.0, unit="arcsec"
                )
            )
        results = run_queries(
            k, queries=queries, query_type="cone_search", n_processes=n_processes
        )

        return results[stream]
    else:
        all_results = {}
        max_queries_per_batch = min(int(max_queries_per_batch), len(epochs))
        n_batches = int(len(epochs) / max_queries_per_batch)
        with tqdm(total=n_batches * max_queries_per_batch, disable=not verbose) as pbar:
            for i in range(n_batches):
                queries = []
                for j, epoch in enumerate(
                    epochs[
                        i
                        * max_queries_per_batch : (i + 1)  # noqa E203
                        * max_queries_per_batch
                    ]
                ):
                    objects = {}
                    catalog_parameters = {
                        stream: {
                            "filter": {
                                "candidate.jd": {
                                    "$gte": epoch - 0.01,
                                    "$lte": epoch + 0.01,
                                }
                            },
                            "projection": {
                                "_id": 0,
                                "candid": 1,
                                "objectId": 1,
                                "candidate.jd": 1,
                            },
                        }
                    }
                    for obj_name in objects_with_positions:
                        objects[obj_name] = [
                            objects_with_positions[obj_name]["ra"][
                                i * max_queries_per_batch + j
                            ],
                            objects_with_positions[obj_name]["dec"][
                                i * max_queries_per_batch + j
                            ],
                        ]
                    queries.append(
                        build_cone_search(
                            objects, catalog_parameters, radius=5.0, unit="arcsec"
                        )
                    )
                # run the queries
                results = run_queries(
                    k,
                    queries=queries,
                    query_type="cone_search",
                    n_processes=n_processes,
                )

                if i == 0:
                    all_results = results[stream]
                else:
                    for obj_name in all_results:
                        all_results[obj_name] += results[stream][
                            obj_name
                        ]

                pbar.update(max_queries_per_batch)

        return all_results
