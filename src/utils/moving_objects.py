from astroquery.jplhorizons import Horizons
from astropy.coordinates import SkyCoord
from astropy.time import Time
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm


def _get_object_positions(
    obj_name: str, start_date: str, end_date: str, time_step: str
):
    obj = Horizons(
        id=obj_name, epochs={"start": start_date, "stop": end_date, "step": time_step}
    )
    try:
        eph = obj.ephemerides()
        pos = SkyCoord(eph["RA"], eph["DEC"], unit="deg")
        times = Time(np.asarray(eph["datetime_jd"]), format="jd", scale="utc")
        ra, dec, times = np.asarray(pos.ra), np.asarray(pos.dec), times
        del obj, eph, pos
    except Exception as e:
        print(f"(error: {str(e)})")
        ra, dec, times = np.asarray("0"), np.asarray("0"), np.asarray("0")
    return ra, dec, times


def get_object_positions(
    obj_name: str,
    start_date: str,
    end_date: str,
    time_step: str,
    verbose: bool,
):
    # here we make sure to batch the requests in start_date -> end_date windows
    # less than 1 year long to avoid timeouts
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    date_diff = end_date - start_date
    if date_diff.days > 365:
        # split into smaller windows
        date_windows = []
        for i in range(date_diff.days // 365 + 1):
            date_windows.append(
                (
                    start_date + timedelta(days=i * 365),
                    start_date + timedelta(days=(i + 1) * 365),
                )
            )
    else:
        date_windows = [(start_date, end_date)]

    ra, dec, jd = [], [], []
    iterator = tqdm(
        date_windows,
        desc=f"Fetching {obj_name} positions (batched per year if needed)",
        disable=not verbose,
    )
    for start, end in iterator:
        ra_, dec_, times = _get_object_positions(
            obj_name=obj_name,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            time_step=time_step,
        )
        ra.extend(ra_)
        dec.extend(dec_)
        jd.extend([t.jd for t in times])

    return {"ra": ra, "dec": dec, "jd": jd}
