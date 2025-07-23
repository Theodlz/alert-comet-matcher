from collections import defaultdict
from penquins import Kowalski

from src.config import load_config
from src.utils.validate import KowalskiCredentials

cfg = load_config()


def get_credentials():
    return KowalskiCredentials(
        protocol=cfg["kowalski.protocol"],
        host=cfg["kowalski.host"],
        port=int(cfg["kowalski.port"]),
        token=cfg["kowalski.token"],
    )


def connect_kowalski(
    credentials: KowalskiCredentials | dict, verbose: bool = False, timeout: int = 6000
):
    """Connect to Kowalski

    Args:
        credentials (KowalskiCredentials): Kowalski credentials
        verbose (bool, optional): verbose. Defaults to False.
        timeout (int, optional): timeout. Default to 6000.

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
    kowalski: Kowalski,
    queries: list[dict],
    query_type: str,
    n_processes: int,
    stream: str,
    seen_ids_by_comet: dict[str, set[str]],
):
    """Run queries in Kowalski and return results without duplicates

    Args:
        kowalski (Kowalski): Kowalski client
        queries (list[dict]): list of queries
        query_type (str): query type. One of 'cone_search', 'near', 'aggregate'
        n_processes (int, optional): number of processes.
        stream (str): alert stream name
        seen_ids_by_comet (dict[str, set()]): dict with comet name as key and set of seen ids as value

    Returns:
        query_results (dict): query results
    """
    responses = kowalski.query(
        queries=queries, use_batch_query=True, max_n_threads=n_processes
    )
    if query_type != "cone_search":
        raise NotImplementedError(f"query_type {query_type} not implemented yet!")

    results = defaultdict(list)
    for query_result_list in responses.values():
        for query_result in query_result_list:
            for comet, alerts in query_result["data"][stream].items():
                # Filter alerts to only keep those not already seen
                new_items = [
                    alert
                    for alert in alerts
                    if alert["candid"] not in seen_ids_by_comet.get(comet, set())
                ]
                # Update seen ids
                seen_ids_by_comet.setdefault(comet, set()).update(
                    alert["candid"] for alert in new_items
                )
                results[comet].extend(new_items)
    return results
