import requests
import json
from time import sleep

MAX_TIMEOUT = 300 # Time in seconds

def contentRedownloader():
    # Sonarr configuration values
    print("\n  ** ex) http://192.168.86.20:8989")
    sonarr_url = input("Sonarr URL: ")
    api_key = input("Sonarr API key: ")

    # Get a list of all series
    get_series_url = sonarr_url + "/api/series?apikey=" + api_key
    get_series_response = requests.get(get_series_url)
    if get_series_response.status_code != 200: 
        print("Failed communication with Sonarr!")
        return False
    series_list = json.loads(get_series_response.content) # Turn the JSON into Python

    # Additional configuration values
    print("\n  ** ex) /media/TV    or    /media    or    /")
    root_dir = input("Directory path to upgrade: ")
    max_episodes = input("Max amount of episodes to search per series (optional): ")
    if (int(max_episodes) <= 0) or (max_episodes == ""):
        max_episodes = 1000000
    starting_series = input("Series name to start at (optional): ")

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
            else:
                starting_series = ""
            if series['episodeCount'] > int(max_episodes):
                print("Too many episodes. Skipping...")
                continue
            
            # Command Sonarr to search
            command_search_url = sonarr_url + "/api/command?apikey=" + api_key
            command_search_parameters = {"name":"SeriesSearch", "seriesId":int(series['id'])}
            command_search_response = requests.post(command_search_url, json.dumps(command_search_parameters))
            if command_search_response.status_code != 201:
                print("Search command failed!")
                return False
            command_search_id = json.loads(command_search_response.content)['id']

            # Wait for the search to complete
            completion_url = sonarr_url + "/api/command/" + str(command_search_id) + "?apikey=" + api_key
            timeout_counter = 0
            while True:
                sleep(1)
                timeout_counter = timeout_counter + 1
                completion_response = requests.get(completion_url)
                if completion_response.status_code != 200:
                    print("Completion check failed!")
                    return False                                
                elif json.loads(completion_response.content)['state'] == "completed":
                    break
                elif timeout_counter > MAX_TIMEOUT:
                    print("Took too long... moving on to the next series.")
                    break

if __name__ == "__main__":
    contentRedownloader()
