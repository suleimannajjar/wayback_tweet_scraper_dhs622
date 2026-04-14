import csv

import requests
import pandas as pd
import re
from tqdm import tqdm
import os
from config import DATA_DIR


def sanitize_status_code(status_code: str) -> int:
    try:
        return int(status_code)
    except:
        return 0


def is_retrievable(status_code: str) -> bool:
    return int(status_code) == 200


def parse_tweet_id_from_snapshot_url(url: str, target_handle: str):
    url_stub = f"twitter.com/{target_handle.lower()}/status/"
    remainder = url[url.lower().find(url_stub) + len(url_stub) :]

    match = re.match("\d+", remainder)

    if match is None:
        return None

    return remainder[match.span()[0] : match.span()[1]]


if __name__ == "__main__":
    # Input:
    target_handle = "kyliejanekremer"

    #################################################################################

    target_url = f"https://twitter.com/{target_handle}/status"
    print(f"looking up Wayback Machine tweet snapshots for @{target_handle}")

    wayback_url = (
        f"http://web.archive.org/cdx/search/cdx?url={target_url}/*&output=json"
    )

    # Query Wayback Machine for snapshots:
    resp = requests.get(wayback_url)

    if resp.status_code != 200:
        raise Exception(f"received response code {resp.status_code}")

    snapshots = resp.json()
    headers = snapshots.pop(0)
    df = pd.DataFrame(snapshots, columns=headers)

    # Filter down to retrievable snapshots:
    df["statuscode"] = df["statuscode"].apply(lambda x: sanitize_status_code(x))
    df["retrievable"] = df["statuscode"].apply(lambda x: is_retrievable(x))
    df = df.loc[df["retrievable"] == True, :]

    # Filter down to latest retreivable snapshot per tweet ID:
    df["tweet_id"] = df["original"].apply(
        lambda url: parse_tweet_id_from_snapshot_url(url, target_handle)
    )
    df = df.loc[df["tweet_id"].notnull(), :]

    # Per tweet ID, sort snapshots in reverse-chronological order:
    df.sort_values(["tweet_id", "timestamp"], ascending=[True, False], inplace=True)

    df.to_csv(os.path.join(DATA_DIR, f"snapshots_wayback_tweets_{target_handle}.csv"), index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONNUMERIC)
    print(f"{df.shape[0]} snapshots to investigate for @{target_handle}.")