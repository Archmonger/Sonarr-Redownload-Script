import json
from time import sleep

import requests

MAX_TIMEOUT = 3600 # Time in seconds
MAX_CONNECTION_RETRIES = 10 # Number of attempts
CONNECTION_RETRY_TIMEOUT = 10 # Time in seconds

def content_redownloader():
    """Queries sonarr to upgrade content that match given parameters"""
    # Sonarr configuration values
    print("\n  ** ex) http://192.168.86.20:8989")
    sonarr_url = str(input("Sonarr URL: "))
    api_key = str(input("Sonarr API key: "))

    # Get a list of all series
    get_series_url = sonarr_url + "/api/series?apikey=" + api_key
    get_series_response = requests.get(get_series_url)
    connection_retries = 0
    while get_series_response.status_code != 200:
        print("Failed communication with Sonarr!")
        connection_retries = connection_retries + 1
        sleep(CONNECTION_RETRY_TIMEOUT)
        get_series_response = requests.get(get_series_url)
        if connection_retries > MAX_CONNECTION_RETRIES:
            return False
    series_list = json.loads(get_series_response.content) # Turn the JSON into Python

    # Additional configuration values
    print("\n  ** ex) /media/TV    or    /media")
    root_dir = str(input("Root directory to upgrade (optional): "))
    max_episodes = input("Skip shows with more than _____ episodes (optional): ")
    try:
        if int(max_episodes) <= 0:
            max_episodes = 1000000
    except ValueError:
        max_episodes = 1000000
    starting_series = input("Show name to start at (optional): ")
    rapid_mode = False
    if str(input("Rapid mode [Y/N] (optional): ")).lower() == "y":
        print("\nWARNING: Rapid immediately queues all search queries. This can overwhelm Sonarr, and is difficult to stop once started.")
        if str(input("Are you sure? [Y/N]: ")).lower() == "y":
            rapid_mode = True

    # Search for file upgrades in the directory
    counter = -1
    for series in series_list:
        if series['path'][:len(root_dir)] == root_dir:
            counter = counter + 1
            print(str(counter) + ": Processing " + series['title'])

            # Skip checks
            if starting_series.lower() != series['title'].lower()[:len(starting_series)]:
                print("Not starting series. Skipping...")
                continue
            starting_series = ""
            if series['episodeCount'] > int(max_episodes):
                print("Show has more episodes than the limit. Skipping...")
                continue

            # Command Sonarr to search
            command_search_url = sonarr_url + "/api/command?apikey=" + api_key
            command_search_parameters = {"name":"SeriesSearch", "seriesId":int(series['id'])}
            command_search_response = requests.post(command_search_url, json.dumps(command_search_parameters))
            connection_retries = 0
            while command_search_response.status_code != 201:
                print("Search command failed!")
                connection_retries = connection_retries + 1
                sleep(CONNECTION_RETRY_TIMEOUT)
                command_search_response = requests.post(command_search_url, json.dumps(command_search_parameters))
                if connection_retries > MAX_CONNECTION_RETRIES:
                    return False
            command_search_id = json.loads(command_search_response.content)['id']

            # Wait for the search to complete
            if not rapid_mode:
                completion_url = sonarr_url + "/api/command/" + str(command_search_id) + "?apikey=" + api_key
                timeout_counter = 0
                while True:
                    sleep(5)
                    timeout_counter = timeout_counter + 5
                    completion_response = requests.get(completion_url)
                    connection_retries = 0
                    while completion_response.status_code != 200:
                        print("Completion check failed!")
                        connection_retries = connection_retries + 1
                        sleep(CONNECTION_RETRY_TIMEOUT)
                        completion_response = requests.get(completion_url)
                        if connection_retries > MAX_CONNECTION_RETRIES:
                            return False
                    if json.loads(completion_response.content)['state'] == "completed":
                        break
                    if timeout_counter > MAX_TIMEOUT:
                        print("Show is still processing after " + str(MAX_TIMEOUT) + " seconds. Starting the next show.")
                        break
    return True

if __name__ == "__main__":
    if content_redownloader():
        print("Script successfully completed.")
    else:
        print("Script failed to complete.")
