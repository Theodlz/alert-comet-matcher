import ray
from ray.util.actor_pool import ActorPool
import time
import numpy as np
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astroquery.jplhorizons import Horizons
import pandas as pd
import os
from penquins import Kowalski
from tqdm import tqdm
import glob
import gzip
import io
from astropy.io import fits
from copy import deepcopy
from astropy.visualization import (
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
)
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import joblib
from dotenv import load_dotenv

load_dotenv()

ray.init(
    include_dashboard=True,
    dashboard_host="0.0.0.0",
    dashboard_port=os.environ.get("PORT", 8265),
    num_cpus=6,
)


def get_comet_names():
    import requests
    from bs4 import BeautifulSoup
    import re

    url = "https://physics.ucf.edu/~yfernandez/cometlist.html"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    option1 = r"([A-Z]\/\d+ [A-Z]+\d+)"
    option2 = r"([A-Z]\/\d+ [A-Z]+\d*)"
    # get all pre tags
    pre_tags = soup.find_all("pre")
    names = []
    for pre in pre_tags:
        for line in pre.text.split("\n")[4:]:
            name = None
            if line == "":
                continue
            # if re.search(option1, line):
            # name = re.search(option1, line).group(0)
            if re.search(option1, line):
                name = re.search(option1, line).group(0)
            elif re.search(option2, line):
                name = re.search(option2, line).group(0)
            if name:
                names.append(name)
    return names


@ray.remote(num_cpus=4)
def update_comet_alerts(kowalski_credentials: dict, make_pdfs=False):
    k = Kowalski(
        protocol=kowalski_credentials.get("protocol", "https"),
        host=kowalski_credentials.get("host", "kowalski.caltech.edu"),
        port=kowalski_credentials.get("port", 443),
        token=kowalski_credentials.get("token", None),
        verbose=True,
    )

    def bulk_cone_searches(
        comet_dfs: list[pd.DataFrame],
        comet_names: list[str],
        existing_alerts_by_comet: dict[str, list] = {},
    ):
        # verify that the dataframes are not empty, and all have the same length
        # and all have the same columns: ra, dec, times
        if any([len(df) == 0 for df in comet_dfs]):
            raise ValueError("One or more of the input dataframes is empty")
        length = len(comet_dfs[0])
        for i, df in enumerate(comet_dfs):
            if len(df) != length:
                raise ValueError(
                    f"Input dataframe {comet_names[i]} has different length than the first dataframe"
                )
        for df in comet_dfs:
            if not set(df.columns).issubset({"ra", "dec", "times"}):
                raise ValueError("Input dataframes have different columns")

        # find the indexes of the alerts that have already been queried
        # for that find the indexes in comet_names that are in existing_alerts_by_comet

        indexes = []
        for i, comet_name in enumerate(comet_names):
            if comet_name not in list(existing_alerts_by_comet.keys()):
                indexes.append(i)
        # remove the dataframes that have already been queried
        comet_dfs = [comet_dfs[i] for i in indexes]
        comet_names = [comet_names[i] for i in indexes]

        if len(comet_dfs) == 0:
            print("All comets have already been queried already")
            return existing_alerts_by_comet

        print(f"Generating queries for {len(comet_dfs)} comets...")

        queries = []
        length = len(comet_dfs[0])
        for i in tqdm(range(length)):
            objects = {}
            for j in range(len(comet_dfs)):
                objects[comet_names[j]] = [
                    comet_dfs[j]["ra"][i],
                    comet_dfs[j]["dec"][i],
                ]
            query = {
                "query_type": "cone_search",
                "query": {
                    "object_coordinates": {
                        "radec": objects,
                        "cone_search_radius": 5,
                        "cone_search_unit": "arcsec",
                    },
                    "catalogs": {
                        "ZTF_alerts": {
                            "filter": {
                                "candidate.jd": {
                                    "$gte": comet_dfs[0]["times"][i] - 0.01,
                                    "$lte": comet_dfs[0]["times"][i] + 0.01,
                                },
                            },
                            "projection": {
                                # "cutoutScience": 0,
                                "_id": 0,
                                "candid": 1,
                                "objectId": 1,
                                "candidate.jd": 1,
                            },
                        }
                    },
                },
            }
            queries.append(query)

        results = k.query(queries=queries, use_batch_query=True, max_n_threads=20)

        alerts_per_comet = {}
        for comet_name in comet_names:
            alerts_per_comet[comet_name] = []

        for result in results["default"]:
            for comet_name in comet_names:
                alerts_per_comet[comet_name].extend(
                    result["data"]["ZTF_alerts"][comet_name]
                )

        # merge it with the existing alerts
        alerts_per_comet = {**alerts_per_comet, **existing_alerts_by_comet}

        # then deduplicate the alerts by candid
        for comet_name in comet_names:
            candid_list = []
            for alert in alerts_per_comet[comet_name]:
                candid_list.append(alert["candid"])
            candid_list = list(set(candid_list))
            alerts = []
            for alert in alerts_per_comet[comet_name]:
                if alert["candid"] in candid_list:
                    alerts.append(alert)
                    candid_list.remove(alert["candid"])
            alerts_per_comet[comet_name] = alerts

        # cleanup after ourselves
        del results, queries

        return alerts_per_comet

    def grab_candidate_data(
        alerts_by_comets_full: dict, existing_alerts_by_comets_full: dict
    ):
        queries = []
        for comet in alerts_by_comets_full:
            if comet in existing_alerts_by_comets_full:
                continue
            for alert in alerts_by_comets_full[comet]:
                query = {
                    "query_type": "find",
                    "query": {
                        "catalog": "ZTF_alerts",
                        "filter": {"candid": alert["candid"]},
                        "projection": {
                            "_id": 0,
                            "objectId": 1,
                            "candid": 1,
                            "candidate": 1,
                            "cutoutScience": 1,
                            "cutoutTemplate": 1,
                            "cutoutDifference": 1,
                            "classifications": 1,
                        },
                    },
                }
                queries.append(query)

        if len(queries) == 0:
            print("All alerts data has been fetched already. Skipping...")
            return existing_alerts_by_comets_full

        print(f"Grabbing candidate data for {len(queries)} alerts...")

        result_cutouts = k.query(
            queries=queries, use_batch_query=True, max_n_threads=20
        )
        alerts_and_cutouts = []
        for r in result_cutouts["default"]:
            alerts_and_cutouts.append(r["data"][0])

        for comet in alerts_by_comets_full:
            for alert in alerts_by_comets_full[comet]:
                for alert_and_cutout in alerts_and_cutouts:
                    if alert_and_cutout["candid"] == alert["candid"]:
                        alert["cutoutScience"] = alert_and_cutout["cutoutScience"]
                        alert["cutoutTemplate"] = alert_and_cutout["cutoutTemplate"]
                        alert["cutoutDifference"] = alert_and_cutout["cutoutDifference"]
                        alert["objectId"] = alert_and_cutout["objectId"]
                        alert["rb"] = alert_and_cutout["candidate"]["rb"]
                        alert["drb"] = alert_and_cutout["candidate"].get("drb", None)
                        alert["classifications"] = alert_and_cutout["classifications"]
                        alert["candidate"] = alert_and_cutout["candidate"]
                        break

        return alerts_by_comets_full

    def generate_pdfs(alert_with_cutouts_by_comets):
        if not os.path.exists("comet_data/pdfs"):
            os.makedirs("comet_data/pdfs")
        for comet in alert_with_cutouts_by_comets:
            # create a pdf with all the thumbnails, one line per object, each with 3 cutouts and the objectId, candid, and jd
            if len(alert_with_cutouts_by_comets[comet]) == 0:
                continue
            if os.path.exists(f"comet_data/pdfs/{comet}_171107_231107_10m.pdf"):
                continue
            with PdfPages(f"comet_data/pdfs/{comet}_171107_231107_10m.pdf") as pdf:
                print(f"Creating pdf for {comet}")
                candid_plotted = []
                # first sort the alerts by jd (lowest first)
                alert_with_cutouts_by_comets[comet] = sorted(
                    alert_with_cutouts_by_comets[comet],
                    key=lambda x: x["candidate"]["jd"],
                )
                for alert in alert_with_cutouts_by_comets[comet]:
                    if alert["candid"] in candid_plotted:
                        continue
                    candid_plotted.append(alert["candid"])
                    fig, axs = plt.subplots(1, 3, figsize=(15, 5))
                    for i, cutout_type in enumerate(["new", "ref", "sub"]):
                        thumbnail_data = alert["thumbnails"][cutout_type]
                        axs[i].imshow(
                            thumbnail_data[0],
                            cmap="bone",
                            origin="lower",
                            vmin=thumbnail_data[1],
                            vmax=thumbnail_data[2],
                        )
                        # i get the error Image data of dtype object cannot be converted to float
                    title = f"objectId: {alert['objectId']}, candid: {alert['candid']}, jd: {alert['candidate']['jd']}"
                    title += f" (rb: {alert['rb']}, drb: {alert['drb']})"
                    fig.suptitle(f"{title}")
                    pdf.savefig(fig)
                    plt.close(fig)

    def make_thumbnail(alert, alert_packet_type: str):
        """
        Convert lossless FITS cutouts from ZTF-like alerts into PNGs

        :param alert: ZTF-like alert packet/dict
        :param skyportal_type: <new|ref|sub> thumbnail type expected by SkyPortal
        :param alert_packet_type: <Science|Template|Difference> survey naming
        :return:
        """
        alert = deepcopy(alert)

        cutout_data = alert[f"cutout{alert_packet_type}"]
        cutout_data = cutout_data["stampData"]
        with gzip.open(io.BytesIO(cutout_data), "rb") as f:
            with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                image_data = hdu[0].data

        # Survey-specific transformations to get North up and West on the right
        image_data = np.flipud(image_data)

        # replace nans with median:
        img = np.array(image_data)
        # replace dubiously large values
        xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
        if img[xl].any():
            img[xl] = np.nan
        if np.isnan(img).any():
            median = float(np.nanmean(img.flatten()))
            img = np.nan_to_num(img, nan=median)

        norm = ImageNormalize(
            img,
            stretch=LinearStretch()
            if alert_packet_type == "Difference"
            else LogStretch(),
        )
        img_norm = norm(img)
        normalizer = AsymmetricPercentileInterval(
            lower_percentile=1, upper_percentile=100
        )
        vmin, vmax = normalizer.get_limits(img_norm)
        return img_norm, vmin, vmax

    while True:
        # load the existing joblib file, if any
        try:
            with open("comet_data/alerts_by_comets.joblib", "rb") as f:
                existing_alerts_by_comets = joblib.load(f)
        except FileNotFoundError:
            existing_alerts_by_comets = {}

        print(f"Found {len(existing_alerts_by_comets)} comets with alerts already")

        # grab the list of CSVs in the current directory
        csvs = glob.glob("comet_data/csvs/*.csv")
        # keep only those that contain "171107"
        csvs = [csv for csv in csvs if not csv.startswith("alert") and "171107" in csv]
        print(f"Found {len(csvs)} CSVs to process")

        dataframes = []
        names = []
        for i, f in enumerate(csvs):
            comet_name = f.split("_17")[0]
            df = pd.read_csv(f)
            dataframes.append(df)
            names.append(comet_name.split("comet_data/csvs/")[1])

        alerts_by_comets = bulk_cone_searches(
            dataframes, names, existing_alerts_by_comets
        )
        del existing_alerts_by_comets

        joblib.dump(alerts_by_comets, "comet_data/alerts_by_comets.joblib")

        try:
            with open("comet_data/alerts_by_comets_full.joblib", "rb") as f:
                existing_alerts_by_comets_full = joblib.load(f)
        except FileNotFoundError:
            existing_alerts_by_comets_full = {}

        alerts_by_comets_full = grab_candidate_data(
            alerts_by_comets, existing_alerts_by_comets_full
        )
        del existing_alerts_by_comets_full

        print(f"Generating thumbnails for {len(alerts_by_comets_full)} comets...")

        for comet in alerts_by_comets_full:
            for i, alert in enumerate(alerts_by_comets_full[comet]):
                if "thumbnails" not in alert:
                    alerts_by_comets_full[comet][i]["thumbnails"] = {
                        "new": make_thumbnail(alert, "Science"),
                        "ref": make_thumbnail(alert, "Template"),
                        "sub": make_thumbnail(alert, "Difference"),
                    }

        joblib.dump(alerts_by_comets_full, "comet_data/alerts_by_comets_full.joblib")

        if make_pdfs:
            generate_pdfs(alerts_by_comets_full)

        print("Done! Sleeping for 60 seconds...")
        time.sleep(60)


@ray.remote(num_cpus=1)
class CometDataFetcher:
    def __init__(self):
        pass

    def create_comet_data(self, comet_name):
        def get_current_pos_date_range(object_name, start_date, end_date, time_step):
            obj = Horizons(
                id=object_name,
                epochs={"start": start_date, "stop": end_date, "step": time_step},
            )
            # print(f"Getting the obj took {time.time()-timer} seconds")
            try:
                eph = obj.ephemerides()
                # print(f"Getting the ephemerides took {time.time()-timer} seconds")
                pos = SkyCoord(eph["RA"], eph["DEC"], unit="deg")
                times = Time(np.asarray(eph["datetime_jd"]), format="jd", scale="utc")
                ra, dec, times = np.asarray(pos.ra), np.asarray(pos.dec), times
                del obj, eph, pos
            except Exception as e:
                print(f"(error: {str(e)})")
                # print ('Too many object entries with the same name in JPL database. Need to use record number as object name, e.g., 90000031')
                ra, dec, times = np.asarray("0"), np.asarray("0"), np.asarray("0")
            return ra, dec, times

        try:
            file_name = f'comet_data/csvs/{comet_name.replace("/", "_").replace(" ", "_")}_171107_231107_10m.csv'
            if os.path.exists(file_name):
                print(f"Skipping {comet_name}, already exists")
                return True
            all_ras, all_decs, all_times = [], [], []
            for i, year in enumerate(range(2017, 2023)):
                print(f"Getting {comet_name} data {i+1}/6...")
                start_date = f"{year}-11-07"
                end_date = f"{year+1}-11-07"
                time_step = "10m"
                ra_deg, dec_deg, times = get_current_pos_date_range(
                    comet_name, start_date, end_date, time_step
                )
                all_ras.extend(ra_deg)
                all_decs.extend(dec_deg)
                all_times.extend(times)
                del ra_deg, dec_deg, times
            # save the ra, dec, and times in a dataframe and then save it as a csv file
            df = pd.DataFrame(
                {
                    "ra": all_ras,
                    "dec": all_decs,
                    "times": [all_time.jd for all_time in all_times],
                }
            )
            df.to_csv(file_name, index=False)
            del df
            return True
        except Exception as e:
            print(e)
            return False


kowalski_credentials = {
    "protocol": os.environ.get("KOWALSKI_PROTOCOL", "https"),
    "host": os.environ.get("KOWALSKI_HOST", "kowalski.caltech.edu"),
    "port": os.environ.get("KOWALSKI_PORT", 443),
    "token": os.environ.get("KOWALSKI_TOKEN", None),
}

if not kowalski_credentials["token"]:
    raise ValueError("Kowalski token not set")

if not os.path.exists("comet_data"):
    os.makedirs("comet_data")
if not os.path.exists("comet_data/csvs"):
    os.makedirs("comet_data/csvs")
if not os.path.exists("comet_data/pdfs"):
    os.makedirs("comet_data/pdfs")

comet_list = ["P/2021 A3", "C/2020 T2", "C/2022 E3"]
comet_list.extend(get_comet_names())
comet_list = sorted(list(set(comet_list)), reverse=True)

print(f"Found {len(comet_list)} comets to query for")

# create a task for updating the comet alerts
update_comet_alerts_task = update_comet_alerts.remote(
    kowalski_credentials=kowalski_credentials, make_pdfs=True
)
# we dont need to wait for it to finish, it will run in the background in a loop

# we want to be fetching alert data for at most 2 comets at a time
# so we create a pool of 2 actors
actor_pool = ActorPool([CometDataFetcher.remote() for _ in range(2)])

for comet_name in comet_list:
    actor_pool.submit(lambda a, c: a.create_comet_data.remote(c), comet_name)

# wait for the actor pool to finish, meaning that the actor_pool.has_next() returns False
# if there is a task scheduled, call get_next_unordered() to get the results and let the actor pool continue
while actor_pool.has_next():
    actor_pool.get_next_unordered()
    time.sleep(1)

# wait for the update_comet_alerts_task to finish
# just a trick to keep it running in the background forever
ray.get(update_comet_alerts_task)
