import json
import os
from tqdm import tqdm

from src.utils.kowalski import build_cone_search, run_queries, Kowalski
from src.utils.paths import comet_alerts_file
from src.config import load_config

cfg = load_config()

def is_epoch_processed(epoch, processed_epochs):
    return processed_epochs["start"] <= epoch <= processed_epochs["end"]


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

    # verify that all the objects have the same epochs
    # if not, raise an exception
    if len(objects_with_positions) > 1:
        epochs = set()
        for obj_name in objects_with_positions:
            epochs.add(tuple(objects_with_positions[obj_name]["jd"]))
        if len(epochs) > 1:
            raise Exception("Objects have different epochs")

    print("Generating queries...")

    # reformat to have a dict with keys as epochs and values as lists of objects and their positions at that epoch
    epochs = tuple(objects_with_positions[list(objects_with_positions.keys())[0]]["jd"])
    max_queries_per_batch = min(max_queries_per_batch, len(epochs)) if max_queries_per_batch else len(epochs)
    n_batches = int(len(epochs) / max_queries_per_batch)
    with tqdm(total=n_batches * max_queries_per_batch, disable=not verbose) as pbar:
        for i in range(n_batches):
            queries = []
            batch_epochs = epochs[i * max_queries_per_batch: (i + 1) * max_queries_per_batch]
            for j, epoch in enumerate(batch_epochs):
                objects = {}
                # skip objects that have already been processed for this epoch
                for obj_name in objects_with_positions:
                    if os.path.exists(comet_alerts_file(obj_name)):
                        with open(comet_alerts_file(obj_name), "r", encoding="utf-8") as f:
                            data = json.load(f)
                        if is_epoch_processed(epoch, data["processed_epochs"]):
                            continue
                    objects[obj_name] = [
                        objects_with_positions[obj_name]["ra"][i * max_queries_per_batch + j],
                        objects_with_positions[obj_name]["dec"][i * max_queries_per_batch + j],
                    ]
                if len(objects) == 0:
                    continue

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

                queries.append(
                    build_cone_search(
                        objects, catalog_parameters, radius=5.0, unit="arcsec"
                    )
                )

            results = run_queries(
                k,
                queries=queries,
                query_type="cone_search",
                n_processes=n_processes,
            )

            # save results to individual comet files
            for obj_name, result in results[stream].items():
                if os.path.exists(comet_alerts_file(obj_name)):
                    with open(comet_alerts_file(obj_name), "r", encoding="utf-8") as file:
                        data = json.load(file)
                    if batch_epochs[-1] > data["processed_epochs"]["end"]:
                        data["processed_epochs"]["end"] = batch_epochs[-1]
                    if batch_epochs[0] < data["processed_epochs"]["start"]:
                        data["processed_epochs"]["start"] = batch_epochs[0]
                    data["results"].extend(result)
                else:
                    data = {
                        "processed_epochs": {"start": batch_epochs[0], "end": batch_epochs[-1]},
                        "results": result,
                    }

                with open(comet_alerts_file(obj_name), "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

            pbar.update(max_queries_per_batch)

    return