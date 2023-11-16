This is a work in progress!

## What is it about?
This is a fairly simple system, which aims to find the ZTF alerts counterparts for a list of comets from https://physics.ucf.edu/~yfernandez/cometlist.html.

To do so, we use JPL's astroquery to retrieve the positions of said comets for a period of 6 years (from late 2017 to November 2023), and then we use penquins to query Kowalski using cone searches, with somewhat loose queries (in radius and time) to try to compensate for the fixed 10m rate at which we sampled the comets' positions. These querying conditions could be easily modified to be more or less strict.

We use Ray-core to parallelize the comets' positions fetching, and the alert retrieval process.
Due to JPL's API restrictions, we can only fetch for 2 comets at a time.

The Kowalski credentials are passed using python-dotenv, so you need to create a .env file with the following variables:
```bash
KOWALSKI_PROTOCOL=https
KOWALSKI_HOST=kowalski.caltech.edu
KOWALSKI_PORT=443
KOWALSKI_TOKEN=<replace_with_your_token>
```
Also, these can be passed as environment variables.

Ultimately, we'd like to train a binary classifier to try to automagically find potential comet's in the ZTF alerts stream.

## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
python main.py
```

## Deploy on Heroku (optional)

We provide a quick Dockerfile and a heroku.yml file to deploy this app on Heroku, if you'd like to have it run somewhere in the cloud continuously. This hasn't been tested yet.
