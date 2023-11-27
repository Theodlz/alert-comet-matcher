This is a work in progress!

## What is it about?
This is a fairly simple system, which aims to process alert data from multiple surveys in a generic manner, submitting computation to a [Ray cluster](https://www.ray.io/).

The idea is that this system allows to easily start a ray cluster, and submit any task to it. In the future, using ray's distributed features, we can imagine spawning multiple clusters across multiple machines to scale up the computation power super easily.

All the credentials and configuration are stored in a config file, which is not included in this repo, but which you can easily create by copying the `config.defaults.yaml` file and filling in the blanks.

The very first application for this system, and which serves as the basic example aims to find the ZTF alerts counterparts for a list of comets from https://physics.ucf.edu/~yfernandez/cometlist.html. To do so, we use JPL's astroquery to retrieve the positions of said comets for a period of 6 years (from late 2017 to November 2023), and then we use penquins to query Kowalski using cone searches, with somewhat loose queries (in radius and time) to try to compensate for the fixed 10m rate at which we sampled the comets' positions. These querying conditions could be easily modified to be more or less strict.

Ultimately, we'd like to train a binary classifier to try to automagically find potential comet's in the ZTF alerts stream.

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

Example:
First we submit a long running job (without waiting for it to finish) to generate the comets positions for a period of 6 years:
```bash
python main.py submit_job --job_file=scripts/generate_comets_positions.py --nowait
```

Then, at anytime after that, we submit a job that tries to fetch the ZTF alerts for those comets from Kowalski:
```bash
python main.py submit_job --job_file=scripts/fetch_comet_alerts.py
```
