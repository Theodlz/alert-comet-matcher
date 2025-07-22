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
    stream = cfg["kowalski"]["alert_stream"]
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

    obj_processed_epochs = {}
    seen_ids = set()
    for obj_name in objects_with_positions:
        if os.path.exists(comet_alerts_file(obj_name)):
            with open(comet_alerts_file(obj_name), "r", encoding="utf-8") as f:
                data = json.load(f)
            obj_processed_epochs[obj_name] = data["processed_epochs"]
            if data["results"]:
                seen_ids.update(alert["candid"] for alert in data["results"])
        else:
            obj_processed_epochs[obj_name] = set()

    # reformat to have a dict with keys as epochs and values as lists of objects and their positions at that epoch
    epochs = tuple(objects_with_positions[list(objects_with_positions.keys())[0]]["jd"])
    max_queries_per_batch = (
        min(max_queries_per_batch, len(epochs))
        if max_queries_per_batch
        else len(epochs)
    )
    with tqdm(total=len(epochs), disable=not verbose) as pbar:
        # process batch of max_queries_per_batch epochs at a time until all epochs are processed
        for i in range(0, len(epochs), max_queries_per_batch):
            queries = []
            batch_epochs = epochs[i : i + max_queries_per_batch]
            for j, epoch in enumerate(batch_epochs):
                objects = {}
                for obj_name in objects_with_positions:
                    if not obj_processed_epochs[obj_name] or not is_epoch_processed(
                        epoch, obj_processed_epochs[obj_name]
                    ):
                        objects[obj_name] = [
                            objects_with_positions[obj_name]["ra"][i + j],
                            objects_with_positions[obj_name]["dec"][i + j],
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

            # Retrieve data from Kowalski deduplicated by seen candids
            results = run_queries(
                k,
                queries=queries,
                query_type="cone_search",
                n_processes=n_processes,
                stream=stream,
                seen_ids=seen_ids,
            )

            # save alerts to comet alerts files
            for obj_name, alerts in results.items():
                if not os.path.exists(comet_alerts_file(obj_name)):
                    data = {
                        "processed_epochs": {
                            "start": batch_epochs[0],
                            "end": batch_epochs[-1],
                        },
                        "results": alerts,
                    }
                else:
                    with open(comet_alerts_file(obj_name), "r", encoding="utf-8") as f:
                        data = json.load(f)
                    data["processed_epochs"]["start"] = min(
                        data["processed_epochs"]["start"], batch_epochs[0]
                    )
                    data["processed_epochs"]["end"] = max(
                        data["processed_epochs"]["end"], batch_epochs[-1]
                    )

                    if alerts:
                        data["results"].extend(alerts)

                with open(comet_alerts_file(obj_name), "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

            pbar.update(len(batch_epochs))

    return
