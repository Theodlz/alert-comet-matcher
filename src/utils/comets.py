from datetime import datetime, timedelta

import numpy as np
import requests
import re

from astropy.coordinates import SkyCoord
from astropy.time import Time
from astroquery.jplhorizons import Horizons
from bs4 import BeautifulSoup
from tqdm import tqdm

from src.config import load_config

cfg = load_config()


def fetch_comets():
    r = requests.get(cfg["params.comet_list_url"])
    soup = BeautifulSoup(r.text, "html.parser")

    comet_pattern = re.compile(r"[A-Z]/\d{4} [A-Z]{1,3}\d{0,3}(?:-[A-Z])?")
    comets = []

    for pre in soup.find_all("pre"):
        lines = pre.text.splitlines()[4:]  # skip header lines
        for line in lines:
            match = comet_pattern.search(line)
            if match:
                comets.append(match.group())
    return comets


def get_comet_positions(
    comet_name: str,
    start_date: str,
    end_date: str,
    time_step: str,
    verbose: bool = False,
):
    # here we make sure to batch the requests in start_date -> end_date windows
    # less than 1 year long to avoid timeouts
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Create date windows of at most 1 year
    date_windows = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=365), end_date)
        date_windows.append((current_start, current_end))
        current_start = current_end

    ra, dec, jd = [], [], []
    for start, end in tqdm(
        date_windows, desc=f"Fetching {comet_name} positions", disable=not verbose
    ):
        obj = Horizons(
            id=comet_name,
            epochs={
                "start": start.strftime("%Y-%m-%d"),
                "stop": end.strftime("%Y-%m-%d"),
                "step": time_step,
            },
        )
        try:
            eph = obj.ephemerides()
            pos = SkyCoord(eph["RA"], eph["DEC"], unit="deg")
            times = Time(np.asarray(eph["datetime_jd"]), format="jd", scale="utc")

            ra.extend(pos.ra.value.tolist())
            dec.extend(pos.dec.value.tolist())
            jd.extend(times.jd.tolist())
        except Exception as e:
            print(
                f"Error fetching data for {comet_name} between {start.date()} and {end.date()}: {e}"
            )
            return None

    return {"ra": ra, "dec": dec, "jd": jd}
