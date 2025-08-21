import json
import os

from tqdm import tqdm
from collections import defaultdict

from src.config import load_config
from src.fetch_matching_alerts.load_fetched_comets import (
    load_fetched_comets,
    is_epoch_processed,
)
from src.utils.kowalski import build_cone_search, run_queries
from src.utils.paths import comet_alerts_file

cfg = load_config()


def fetch_comet_matching_alerts(
    kowalski,
    verbose,
):
    # Load comet positions already fetched
    comets_with_positions = load_fetched_comets(verbose)
    if not comets_with_positions:
        return

    # verify that all the objects have the same epochs, if not raise an exception
    if len(comets_with_positions) > 1:
        epochs = set()
        for comet_name, positions in comets_with_positions.items():
            epochs.add(tuple(positions["jd"]))
        if len(epochs) > 1:
            raise Exception("Objects have different epochs")

    print("Generating queries...")

    # Store already processed epochs and seen ids
    comet_processed_epochs = {}
    seen_ids_by_comet = defaultdict(set)
    for comet_name in comets_with_positions:
        if os.path.exists(comet_alerts_file(comet_name)):
            with open(comet_alerts_file(comet_name), "r", encoding="utf-8") as f:
                data = json.load(f)
            comet_processed_epochs[comet_name] = data["processed_epochs"]
            if data["results"]:
                seen_ids_by_comet[comet_name].update(
                    alert["candid"] for alert in data["results"]
                )
        else:
            comet_processed_epochs[comet_name] = set()

    # reformat to have a dict with keys as epochs and values as lists of comets and their positions at that epoch
    epochs = tuple(comets_with_positions[list(comets_with_positions.keys())[0]]["jd"])
    max_queries_per_batch = min(
        cfg.get("params.max_queries_per_batch", len(epochs)), len(epochs)
    )
    stream = cfg["kowalski.alert_stream"]
    with tqdm(total=len(epochs), disable=not verbose) as pbar:
        # process batch of max_queries_per_batch epochs at a time until all epochs are processed
        for i in range(0, len(epochs), max_queries_per_batch):
            queries = []
            batch_epochs = epochs[i : i + max_queries_per_batch]
            for j, epoch in enumerate(batch_epochs):
                comets_to_process = {
                    comet_name: [
                        comets_with_positions[comet_name]["ra"][i + j],
                        comets_with_positions[comet_name]["dec"][i + j],
                    ]
                    for comet_name in comets_with_positions
                    if not comet_processed_epochs[comet_name]
                    or not is_epoch_processed(epoch, comet_processed_epochs[comet_name])
                }

                if not comets_to_process:
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
                        comets_to_process, catalog_parameters, radius=5.0, unit="arcsec"
                    )
                )

            # Retrieve data from Kowalski deduplicated by seen candids
            results = run_queries(
                kowalski,
                queries=queries,
                query_type="cone_search",
                n_processes=cfg.get("kowalski.number_of_processes"),
                stream=stream,
                seen_ids_by_comet=seen_ids_by_comet,
            )

            # save alerts to comet alerts files
            for comet_name, alerts in results.items():
                if not os.path.exists(comet_alerts_file(comet_name)):
                    data = {
                        "processed_epochs": {
                            "start": batch_epochs[0],
                            "end": batch_epochs[-1],
                        },
                        "results": alerts,
                    }
                else:
                    with open(
                        comet_alerts_file(comet_name), "r", encoding="utf-8"
                    ) as f:
                        data = json.load(f)
                    data["processed_epochs"]["start"] = min(
                        data["processed_epochs"]["start"], batch_epochs[0]
                    )
                    data["processed_epochs"]["end"] = max(
                        data["processed_epochs"]["end"], batch_epochs[-1]
                    )

                    if alerts:
                        data["results"].extend(alerts)

                with open(comet_alerts_file(comet_name), "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)

            pbar.update(len(batch_epochs))
