import pandas as pd
from tqdm import tqdm
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime, UTC
import json
import os
from config import DATA_DIR

GIVE_UP_SECONDS = 32

def parse_upvote_metrics(tweet_container, tweet_id: str) -> dict:
    upvote_metrics = {}

    for upvote_var in ["reply", "retweet", "favorite"]:
        elements = tweet_container.find_all(
            id=f"profile-tweet-action-{upvote_var}-count-aria-{tweet_id}"
        )

        if len(elements) == 0:
            upvote_metrics[upvote_var] = 0
        elif len(elements) == 1:
            upvote_count_text = elements[0].text
            upvote_metrics[upvote_var] = upvote_count_text
        else:
            raise Exception(
                f"unexpected number of HTML elements named "
                f"profile-tweet-action-{upvote_var}-count-aria-{tweet_id}"
            )

    return upvote_metrics


def parse_tweet_datetime(tweet_container):
    tweet_times = tweet_container.find_all(class_="time")
    assert len(tweet_times) == 1
    tweet_time = tweet_times[0]
    my_timestamp = int(tweet_time.find("span")["data-time"])
    my_datetime = datetime.fromtimestamp(my_timestamp).strftime("%Y-%m-%d %H:%M:%S")
    return {"tweet_created_at": my_datetime}


def parse_tweet_text(tweet_container):
    tweets = tweet_container.find_all(class_="js-tweet-text-container")
    assert len(tweets) == 1
    tweet = tweets[0]
    return {"tweet_text": tweet.text}


def parse_quoted_tweet_author_and_id(quote_tweet_container):
    quote_tweet_links = quote_tweet_container.find_all(class_="QuoteTweet-link js-nav")
    assert len(quote_tweet_links) == 1
    quote_tweet_link = quote_tweet_links[0]
    quote_tweet_link_str = quote_tweet_link.get("href")
    quote_tweet_id = quote_tweet_link_str.split("/")[-1]
    quote_tweet_author = quote_tweet_link_str.split("/")[1].lower()
    return {"quote_tweet_id": quote_tweet_id, "quote_tweet_author": quote_tweet_author}


def parse_quoted_tweet_data(tweet_container):
    quoted_tweet_data = {}

    # Check for quoted tweet:
    quote_tweet_containers = tweet_container.find_all(class_="QuoteTweet-container")
    if len(quote_tweet_containers) > 1:
        raise Exception(
            f"unexpected number of quote-tweet containers ({len(quote_tweet_containers)}"
        )
    elif len(quote_tweet_containers) == 0:
        # No quoted tweet found.
        pass
    else:
        quote_tweet_container = quote_tweet_containers[0]

        # Want to know who is the author of the original tweet, and what did they say.
        quoted_tweet_data.update(
            parse_quoted_tweet_author_and_id(quote_tweet_container)
        )

        # TODO Exercise: parse text of quoted tweet
    return quoted_tweet_data

def parse_tweet_data_from_snapshot_html(html_str: str, tweet_id: str) -> dict:
    tweet_data = {}
    soup = BeautifulSoup(html_str, "lxml")

    tweet_containers = soup.find_all(class_="permalink-inner permalink-tweet-container")
    if len(tweet_containers) != 1:
        raise Exception(
            f"unexpected number of tweet containers ({len(tweet_containers)})"
        )

    tweet_container = tweet_containers[0]

    tweet_data.update(parse_upvote_metrics(tweet_container, tweet_id))
    tweet_data.update(parse_tweet_datetime(tweet_container))
    tweet_data.update(parse_tweet_text(tweet_container))
    tweet_data.update(parse_quoted_tweet_data(tweet_container))

    return tweet_data

def issue_get_request(
    url: str, allow_redirects: bool = True, timeout_seconds: int = 15
) -> requests.Response | None:
    retry_seconds = 1
    with requests.session() as session:
        while True:
            try:
                print(f"Requesting {url}...")
                resp = session.get(
                    url, allow_redirects=allow_redirects, timeout=timeout_seconds
                )
                resp.raise_for_status()
                break
            except requests.exceptions.Timeout:
                print("Request timed out.")
            except requests.exceptions.ConnectionError:
                print("Connection reset by peer.")
            except requests.exceptions.HTTPError as e:
                print(e)
            except Exception as e:
                type(e)
                raise e

            if retry_seconds >= GIVE_UP_SECONDS:
                print("Giving up...")
                return None
            print(f"Retrying after {retry_seconds} seconds...")
            time.sleep(retry_seconds)
            retry_seconds = retry_seconds * 2

        return resp

def retrieve_html(snapshot_url: str) -> str | None:
    resp = issue_get_request(snapshot_url, True, 15)
    if resp is None:
        return None
    return resp.text

if __name__ == "__main__":
    target_handle = "kyliejanekremer"
    big_df = pd.read_csv(os.path.join(DATA_DIR, f"snapshots_wayback_tweets_{target_handle}.csv"))
    output_file_path = os.path.join(DATA_DIR, f"parsed_wayback_tweets_of_{target_handle}.jsonl")
    print(f"{big_df.shape[0]} snapshots to investigate for @{target_handle}.")

    # start working through each unique tweet ID:
    for tweet_id, df in tqdm(big_df.groupby(["tweet_id"])):
        print(f"for tweet_id={tweet_id[0]} there are {df.shape[0]} snapshots.")

        # try retrieving each snapshot...
        for i in df.index:
            timestamp = df.loc[i, "timestamp"]
            original = df.loc[i, "original"]
            snapshot_url = f"https://web.archive.org/web/{timestamp}id_/{original}"

            snapshot_html_str = retrieve_html(snapshot_url)

            if snapshot_html_str is None:
                print(f"Failed to retrieve snapshot :/")
                continue

            if snapshot_html_str.find("Something went wrong, but") != -1:
                print(f"Failed to retrieve snapshot :/")
                continue

            tweet_data_dict = parse_tweet_data_from_snapshot_html(
                snapshot_html_str, tweet_id
            )

            if tweet_data_dict == {}:
                print(f"Failed to parse tweet data from snapshot :/")
                continue

            # Tweet successfully parsed! No need to retrieve and parse other snapshots for this tweet ID
            print(f"Successfully parsed tweet snapshot for tweet_id {tweet_id}")

            # Tag the data before writing to disk:
            metadata_dict = {
                "target_handle": target_handle,
                "tweet_id": tweet_id,
                "snapshot_datetime": datetime.strptime(
                    str(timestamp), "%Y%m%d%H%M%S"
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "snapshot_url": f"https://web.archive.org/web/{timestamp}id_/{original}",
                "parse_datetime": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
            }

            output_data = {
                "data": tweet_data_dict,
                "metadata": metadata_dict,
            }

            # Save tweet_data to disk:
            with open(output_file_path, "a") as my_file:
                my_file.write(json.dumps(output_data) + "\n")

            # Exit for loop:
            break