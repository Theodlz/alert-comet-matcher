from collections import defaultdict
from penquins import Kowalski
import os

from src.config import load_config
from src.utils.validate import KowalskiCredentials

cfg = load_config()


def get_credentials():
    return KowalskiCredentials(
        protocol=cfg.get("kowalski.protocol", os.getenv("KOWALSKI_PROTOCOL")),
        host=cfg.get("kowalski.host", os.getenv("KOWALSKI_HOST")),
        port=int(cfg.get("kowalski.port", os.getenv("KOWALSKI_PORT"))),
        token=cfg.get("kowalski.token", os.getenv("KOWALSKI_TOKEN")),
    )


def connect_kowalski(
    credentials: KowalskiCredentials | dict, verbose: bool = False, timeout: int = 6000
):
    """Connect to Kowalski

    Args:
        credentials (KowalskiCredentials): Kowalski credentials
        verbose (bool, optional): verbose. Defaults to False.
        timeout (int, optional): timeout. Defaults to 6000.

    Returns:
        Kowalski: Kowalski client
    """
    if isinstance(credentials, dict):
        credentials = KowalskiCredentials(**credentials)
    kowalski = Kowalski(
        protocol=credentials.protocol,
        host=credentials.host,
        port=str(credentials.port),
        token=credentials.token,
        verbose=verbose,
        timeout=timeout,
    )
    return kowalski


def get_kowalski(verbose: bool = False):
    """Get Kowalski client

    Returns:
        Kowalski: Kowalski client
    """
    credentials = get_credentials()
    return connect_kowalski(credentials, verbose=verbose)


def build_cone_search(
    objects_with_position: dict,
    catalogs_parameters: dict,
    radius: float = 5.0,
    unit: str = "arcsec",
):
    """Perform cone search in Kowalski

    Args:
        objects_with_position (dict): objects with position
        catalogs_parameters (dict): catalogs parameters
        radius (float, optional): radius. Defaults to 1.0.
        unit (str, optional): unit. Defaults to 'arcsec'.

    Returns:
        dict: query
    """

    return {
        "query_type": "cone_search",
        "query": {
            "object_coordinates": {
                "radec": objects_with_position,
                "cone_search_radius": radius,
                "cone_search_unit": unit,
            },
            "catalogs": catalogs_parameters,
        },
    }


def run_queries(
    k: Kowalski,
    queries: list[dict],
    query_type: str,
    n_processes: int,
    stream: str,
    seen_ids: set,
):
    """Run query in Kowalski and return results without duplicates

    Args:
        k (Kowalski): Kowalski client
        queries (list[dict]): list of queries
        query_type (str): query type. One of 'cone_search', 'near', 'aggregate'
        n_processes (int, optional): number of processes.
        stream (str): alert stream name
        seen_ids (set): set of seen ids to avoid duplicates

    Returns:
        query_results (dict): query results
    """
    responses = k.query(
        queries=queries, use_batch_query=True, max_n_threads=n_processes
    )
    if query_type != "cone_search":
        raise NotImplementedError(f"query_type {query_type} not implemented yet!")

    results = defaultdict(list)
    for query_result_list in responses.values():
        for query_result in query_result_list:
            for obj, obj_data in query_result["data"][stream].items():
                new_items = [
                    item for item in obj_data if item["candid"] not in seen_ids
                ]
                seen_ids.update(item["candid"] for item in new_items)
                results[obj].extend(new_items)
    return results
