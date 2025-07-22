## What is it about? (work in progress)
This is a fairly simple system, which aims to process alert data from multiple surveys in a generic manner,
submitting computation to a [Ray cluster](https://www.ray.io/). The idea is to provide a simple interface
to start a ray cluster, and submit any task to it. In the future, using ray's distributed features,
we can imagine spawning multiple clusters across multiple machines to scale up the computation power super easily.

### Application: Comet Alert Matching
The initial use case is focused on matching alerts from ZTF with known comet orbits.
We use [Astroquery](https://astroquery.readthedocs.io/en/latest/) to calculate comet positions (from 2017 to 2026)
based on data from this [comet list](https://planets.ucf.edu/resources/cometlist/),
and then use [penquins](https://github.com/dmitryduev/penquins) to query Kowalski
for ZTF alerts via cone searches.

- Note: The search radius and time tolerance are intentionally generous to compensate for the fixed 10-minute
 sampling interval of comet positions. These parameters can be easily adjusted.

Ultimately, the goal is to train a binary classifier that can automatically
identify potential comet candidates in the ZTF alert stream.

## Installation
```bash
  pip install -r requirements.txt
```

## Usage

### Start a ray cluster
```bash
  python main.py start
```

### Submit a job to the cluster
```bash
  python main.py submit_job --job_file=scripts/<your_script>.py
```

## Comet Alert Matching Application

### Generate comet positions and store them as Parquet files:
```bash
  python main.py submit_job --job_file=scripts/generate_comets_positions.py --nowait
```
> This script retrieves comet positions over the past 9 years.
> Stop it early if you are testing.

### Fetch ZTF alerts corresponding to those positions and store them as JSON files:
```bash
  python main.py submit_job --job_file=scripts/fetch_comet_alerts.py
```
