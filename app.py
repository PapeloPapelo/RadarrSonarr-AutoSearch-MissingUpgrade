import requests
import random
import logging
import time
import json
import os
from datetime import datetime, timezone
import sys
import threading

######################################
######################################
##############  CONFIGS ##############
######################################
######################################

RADARR_URLS = [			# Make sure the URLS and matching API-Keys are in the same order! First URL = First Key.
    "http://localhost:7879",
    "http://localhost:7878",
]

RADARR_API_KEYS = [
    "1234",
    "1234",
]

SONARR_URLS = [
    "http://localhost:8989",
]

SONARR_API_KEYS = [
    "1324",
]

				# INFO: Disable RSS-Sync-Interval (=0) in Radarr and Sonarr! It will be triggered one instance at a time.
ENABLE_RSS_CIRCLE = True	# or False - ENABLE_SONARR or ENABLE_RADARR are unrelated to RSS.
ENABLE_SONARR = True		# or False
ENABLE_RADARR = True		# or False
ENABLE_DUPE_CHECK = False	# or False When enabled it compares tmdb_id and tvdb_ids between *arr instances and throws a Warning if a movie or show is added twice.
ENABLE_DUPE_DELETION = False	# WARNING! If a radarr duplicate is found the one with the lowest custom score will be fully deleted! Sonarr will only be logged no deletion.

WHAT_TO_SEARCH = "MISSING"	# or UPGRADE NOTE: Upgrade only works for Radarr it will skip Sonarr.

NUM_MOVIES_TO_UPGRADE = 1	# Per Circle this many searches to Tracker will be triggered for each radarr instance.
MAX_SEASONS = 1			# How many Seasons one circle will Search for each Sonarr instance.

CIRCLE_TIMER = 2500		# For one full circle through all enabled moduls Radarr-Sonarr-Rss.
TIME_BETWEEN_RSS_CALLS = 300	# This is the time between each RSS-Instance, once run it will wait the CIRCLE_TIMER.
TIME_BETWEEN_ARR_INSTANCES = 60	# Each instance will trigger NUM_MOVIES_TO_UPGRADE with no further break.

RECENT_SEARCH_DAYS = 7		# Amount of time when a searched item might be searched again earliest, for full fresh start delete 'searched_movies.json'.

######################################
######################################
######################################
######################################
######################################

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='output.log',
    encoding='utf-8',
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO
)

INSTANCES = [
    {
        "type": "radarr",
        "urls": RADARR_URLS,
        "keys": RADARR_API_KEYS,
    },
    {
        "type": "sonarr",
        "urls": SONARR_URLS,
        "keys": SONARR_API_KEYS,
    },
]

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_last_log_entry():
    try:
        with open('output.log', 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else "No logs yet."
    except Exception as e:
        return f"Could not read log file: {e}"

def print_status(radarr_enabled, sonarr_enabled, rss_enabled):
    last_log = get_last_log_entry()
    last_log_trimmed = last_log[:150] + ("..." if len(last_log) > 150 else "")

    banner = [
        "========================================",
        f"RADARR: {'ENABLED' if radarr_enabled else 'DISABLED'}",
        f"SONARR: {'ENABLED' if sonarr_enabled else 'DISABLED'}",
        f"RSS:    {'ENABLED' if rss_enabled else 'DISABLED'}",
        "----------------------------------------",
        f"Last log: {last_log_trimmed}",
        "========================================",
    ]

    clear_screen()
    print("\n".join(banner))

def status_loop():
    while True:
        print_status(
            ENABLE_RADARR,
            ENABLE_SONARR,
            ENABLE_RSS_CIRCLE
        )
        time.sleep(15)

def cleanup_searched_movies():
    searched = load_searched_movies()
    now = time.time()
    for movie_id, timestamp in list(searched.items()):
        if now - timestamp > RECENT_SEARCH_DAYS * 86400:
            del searched[movie_id]
    save_searched_movies(searched)

MOVIE_ENDPOINT = "movie"
MOVIEFILE_ENDPOINT = "moviefile/"
SERIES_ENDPOINT = "series"
SEASON_ENDPOINT = "season"
API_PATH = "/api/v3/"
QUALITY_PROFILE_ENDPOINT = "qualityprofile"
COMMAND_ENDPOINT = "command"
SEARCHED_MOVIES_FILE = "./searched_movies.json"
radarr_headers = {}
EPISODE_ENDPOINT = "episode"
EPISODEFILE_ENDPOINT = "episodefile"
TIMEOUT = 30

################
#### SONARR ####
################

def get_episode_files_by_series(sonarr_url, series_id):
    EPISODEFILE_GET_API_CALL = (
        sonarr_url + API_PATH + EPISODEFILE_ENDPOINT + f"?seriesId={series_id}"
    )
    response = requests.get(
        EPISODEFILE_GET_API_CALL,
        headers=sonarr_headers,
        timeout=TIMEOUT
    )
    response.raise_for_status()
    return {
        ef["id"]: ef
        for ef in response.json()
    }

def get_seasons_to_search(sonarr_url):
    SERIES_GET_API_CALL = sonarr_url + API_PATH + SERIES_ENDPOINT
    response = requests.get(
        SERIES_GET_API_CALL,
        headers=sonarr_headers,
        timeout=TIMEOUT
    )
    response.raise_for_status()

    seasons_to_search = []
    searched = load_searched_movies()

    for series in response.json():
        if len(seasons_to_search) >= MAX_SEASONS:
            break

        series_id = series.get("id")
        if not series_id:
            continue

        episodes = episode_files = None

        for season in series.get("seasons", []):
            if len(seasons_to_search) >= MAX_SEASONS:
                break

            season_number = season.get("seasonNumber")
            if season_number == 0:
                continue
            if not season.get("monitored"):
                continue

            search_key = f"sonarr_{series_id}_{season_number}"
            if search_key in searched:
                continue

            stats = season.get("statistics", {})
            episode_count = stats.get("episodeCount", 0)
            file_count = stats.get("episodeFileCount", 0)

            if episode_count == 0:
                continue

            if WHAT_TO_SEARCH == "MISSING":
                if file_count == 0:
                    seasons_to_search.append((series_id, season_number))

    return seasons_to_search

def search_sonarr_seasons(sonarr_url, seasons):
    SEARCH_API_CALL = sonarr_url + API_PATH + COMMAND_ENDPOINT

    for series_id, season_number in seasons:
        response = requests.post(
            SEARCH_API_CALL,
            headers=sonarr_headers,
            json={
                "name": "SeasonSearch",
                "seriesId": series_id,
                "seasonNumber": season_number
            },
            timeout=TIMEOUT
        )
        response.raise_for_status()

def process_sonarr(sonarr_url):
    global sonarr_headers

    if WHAT_TO_SEARCH.upper() not in ["MISSING"]:
        logger.error("Invalid WHAT_TO_SEARCH value.")
        return

    try:
        api_key = SONARR_API_KEYS[SONARR_URLS.index(sonarr_url)]
    except (ValueError, IndexError):
        logger.error(f"No API key for Sonarr instance {sonarr_url}")
        return

    sonarr_headers = {"X-Api-Key": api_key}

    seasons = get_seasons_to_search(sonarr_url)
    if not seasons:
        logger.info(f"No Sonarr seasons to search for {sonarr_url}.")
        return

    logger.info(f"Sonarr seasons selected for search: {seasons}")

    searched = load_searched_movies()
    now = int(time.time())

    for sid, sn in seasons:
        searched[f"sonarr_{sid}_{sn}"] = now

    save_searched_movies(searched)
    search_sonarr_seasons(sonarr_url, seasons)

################
#### RADARR ####
################

def load_searched_movies():
    if os.path.exists(SEARCHED_MOVIES_FILE):
        with open(SEARCHED_MOVIES_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_searched_movies(data):
    with open(SEARCHED_MOVIES_FILE, 'w') as f:
        json.dump(data, f)

def is_movie_searched_recently(movie_id):
    searched = load_searched_movies()
    last_searched = searched.get(str(movie_id), 0)
    if last_searched == 0:
        return False
    return time.time() - last_searched < RECENT_SEARCH_DAYS * 86400

def get_radarr_quality_cutoff_scores(radarr_url):
    QUALITY_PROFILES_GET_API_CALL = radarr_url + API_PATH + QUALITY_PROFILE_ENDPOINT
    try:
        response = requests.get(QUALITY_PROFILES_GET_API_CALL, headers=radarr_headers, timeout=TIMEOUT)
        response.raise_for_status()
        quality_profiles = response.json()
        quality_to_formats = {}
        for quality in quality_profiles:
            if "cutoffFormatScore" in quality:
                quality_to_formats[quality["id"]] = quality["cutoffFormatScore"]
        return quality_to_formats
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Radarr quality profiles from {radarr_url}: {e}")
        return {}

def get_missing_movies(radarr_url):
    MOVIES_GET_API_CALL = radarr_url + API_PATH + MOVIE_ENDPOINT
    response = requests.get(
        MOVIES_GET_API_CALL,
        headers=radarr_headers,
        timeout=TIMEOUT
    )
    response.raise_for_status()
    return [
        movie for movie in response.json()
        if movie.get("monitored") and not movie.get("hasFile")
    ]

def get_movies_with_files(radarr_url):
    MOVIES_GET_API_CALL = (
        radarr_url + API_PATH + MOVIE_ENDPOINT + "?includeMovieFile=true"
    )
    response = requests.get(
        MOVIES_GET_API_CALL,
        headers=radarr_headers,
        timeout=TIMEOUT
    )
    response.raise_for_status()
    return [
        movie for movie in response.json()
        if movie.get("monitored") and movie.get("movieFile")
    ]

def get_movies_needing_upgrade(movies, quality_to_formats):
    logger.info("Evaluating movies for upgrade eligibility")
    movie_ids = []
    for movie in movies:
        movie_file = movie.get("movieFile")
        if not movie_file:
            continue
        current_score = movie_file.get("customFormatScore", 0)
        cutoff_score = quality_to_formats.get(
            movie["qualityProfileId"], 0
        )
        if current_score < cutoff_score:
            movie_ids.append(movie["id"])
    return movie_ids

def process_radarr(radarr_url):
    movies = []
    missing_movies = []
    global radarr_headers
    url_index = RADARR_URLS.index(radarr_url) if radarr_url in RADARR_URLS else 0
    api_key = RADARR_API_KEYS[url_index] if url_index < len(RADARR_API_KEYS) else RADARR_API_KEYS[0]
    global radarr_headers
    if not api_key:
        logger.error("API key not set for this Radarr instance.")
        return
    radarr_headers = {
        'X-Api-Key': api_key,
    }
    if WHAT_TO_SEARCH.upper() not in ["UPGRADE", "MISSING"]:
        logger.error("Invalid WHAT_TO_SEARCH value. Must be 'UPGRADE' or 'MISSING'.")
        return
    if WHAT_TO_SEARCH == "UPGRADE":
        movies = get_movies_with_files(radarr_url)
        if not isinstance(movies, list):
            logger.error(f"Failed to get movies from {radarr_url}")
            return
        quality_to_formats = get_radarr_quality_cutoff_scores(radarr_url)
        movie_ids = get_movies_needing_upgrade(
            movies,
            quality_to_formats
        )
    elif WHAT_TO_SEARCH == "MISSING":
        missing_movies = get_missing_movies(radarr_url) or []
        if not isinstance(missing_movies, list):
            logger.error(f"Failed to get missing movies from {radarr_url}")
            return
        movie_ids = [movie["id"] for movie in missing_movies]
    else:
        logger.warning(f"Invalid WHAT_TO_SEARCH value: {WHAT_TO_SEARCH}")
        return
    filtered_movie_ids = []
    for movie_id in movie_ids:
        if not is_movie_searched_recently(movie_id):
            filtered_movie_ids.append(movie_id)
    if not filtered_movie_ids:
        logger.info(f"All movies have been searched recently for {radarr_url}. Skipping this cycle.")
        return
    try:    
        if len(filtered_movie_ids) < NUM_MOVIES_TO_UPGRADE:
            random_keys = filtered_movie_ids
        else: 
            random_keys = random.sample(filtered_movie_ids, k=NUM_MOVIES_TO_UPGRADE)
    except ValueError:
        logger.warning("Not enough movies to select for search.")
        return
    if not random_keys:
        logger.warning("No movies selected for search.")
        return
    logger.info(f"Keys to search are {str(random_keys)}")
    searched = load_searched_movies()
    for key in random_keys:
        searched[str(key)] = int(time.time())
    save_searched_movies(searched)
    data = {"name": "MoviesSearch", "movieIds": random_keys}
    SEARCH_MOVIES_POST_API_CALL = radarr_url + API_PATH + COMMAND_ENDPOINT
    try:
        response = requests.post(SEARCH_MOVIES_POST_API_CALL, headers=radarr_headers, json=data, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error searching Radarr movies at {radarr_url}: {e}")

############
### RSS ####
############

def trigger_rss_sync(url, headers):
    SEARCH_API_CALL = url + API_PATH + COMMAND_ENDPOINT
    data = {"name": "RssSync"}
    try:
        response = requests.post(SEARCH_API_CALL, headers=headers, json=data, timeout=TIMEOUT)
        response.raise_for_status()
        logger.info(f"Triggered RSS sync for {url}")
    except Exception as e:
        logger.error(f"Failed to trigger RSS sync for {url}: {e}")

def rss_cycle():
    for radarr_url in RADARR_URLS:
        url_index = RADARR_URLS.index(radarr_url)
        headers = {"X-Api-Key": RADARR_API_KEYS[url_index]}
        trigger_rss_sync(radarr_url, headers)
        time.sleep(TIME_BETWEEN_RSS_CALLS)
    for sonarr_url in SONARR_URLS:
        url_index = SONARR_URLS.index(sonarr_url)
        headers = {"X-Api-Key": SONARR_API_KEYS[url_index]}
        trigger_rss_sync(sonarr_url, headers)
        time.sleep(TIME_BETWEEN_RSS_CALLS)
    logger.info("Completed RSS sync cycle.")

###################
### DUPE CHECK ####
###################

def api_get(url, api_key, endpoint):
    headers = {"X-Api-Key": api_key}
    resp = requests.get(
        url + API_PATH + endpoint,
        headers=headers,
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()

def process_duplicates(urls, keys, endpoint, id_field, instance_type):
    id_map = {}

    for url, key in zip(urls, keys):
        items = api_get(url, key, endpoint)

        for item in items:
            uid = item.get(id_field)
            if not uid:
                continue

            id_map.setdefault(uid, []).append({
                "instance": url,
                "title": item.get("title"),
                "year": item.get("year"),
                "internal_id": item.get("id"),
                "score": item.get("customScore") or 0,  # Radarr only
            })

    # keep only duplicates
    duplicates = {k: v for k, v in id_map.items() if len(v) > 1}

    # deletion logic (Radarr only)
    if ENABLE_DUPE_DELETION and instance_type.lower() == "radarr":
        for uid, entries in duplicates.items():
            # find lowest scored entry
            lowest = min(entries, key=lambda x: x["score"])
            instance_url = lowest["instance"]
            internal_id = lowest["internal_id"]

            try:
                delete_endpoint = f"movie/{internal_id}"
                logger.info(f"Deleting RADARR duplicate '{lowest['title']}' from {instance_url}")
                resp = requests.delete(
                    instance_url + API_PATH + delete_endpoint,
                    headers={"X-Api-Key": keys[urls.index(instance_url)]},
                    params={"deleteFiles": True},
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to delete {lowest['title']} from {instance_url}: {e}")

    return duplicates

def run_duplicate_check():
    """
    Checks duplicates across all INSTANCES.
    Deletes lowest scored Radarr duplicates if ENABLE_DUPE_DELETION=True.
    Logs Sonarr duplicates only.
    """
    result = {
        "radarr": {},
        "sonarr": {},
    }

    for inst in INSTANCES:
        inst_type = inst["type"].lower()
        if inst_type == "radarr":
            result["radarr"] = process_duplicates(
                inst["urls"],
                inst["keys"],
                endpoint="movie",
                id_field="tmdbId",
                instance_type="radarr",
            )
        elif inst_type == "sonarr":
            result["sonarr"] = process_duplicates(
                inst["urls"],
                inst["keys"],
                endpoint="series",
                id_field="tvdbId",
                instance_type="sonarr",
            )
        else:
            raise ValueError(f"Unknown instance type: {inst_type}")

    return result

##############
#### MAIN ####
##############

def main():
    try:
        if not os.path.exists(SEARCHED_MOVIES_FILE):
            with open(SEARCHED_MOVIES_FILE, 'w') as f:
                json.dump({}, f)
        if not RADARR_URLS and not SONARR_URLS:
            logger.warning("No Radarr or Sonarr URLs provided. Exiting.")
            return
    except Exception as e:
        logger.error(f"Could not initialize state: {e}")
        return
    threading.Thread(target=status_loop, daemon=True).start()
    while True:
        try:
            cleanup_searched_movies()
            logger.info(f"Starting new cycle with goal: {WHAT_TO_SEARCH}")
            if ENABLE_RADARR:
                for radarr_url in RADARR_URLS:
                    logger.info(f"Processing Radarr instance: {radarr_url}")
                    process_radarr(radarr_url)
                    time.sleep(TIME_BETWEEN_ARR_INSTANCES)
            if ENABLE_SONARR:
                for sonarr_url in SONARR_URLS:
                    logger.info(f"Processing Sonarr instance: {sonarr_url}")
                    process_sonarr(sonarr_url)
                    time.sleep(TIME_BETWEEN_ARR_INSTANCES)
            if ENABLE_RSS_CIRCLE:
                logger.info("Starting RSS circle.")
                rss_cycle()
            if ENABLE_DUPE_CHECK:
                logger.info("Running duplicate media check.")
                dupe_result = run_duplicate_check()
                radarr_dupes = dupe_result.get("radarr", {})
                sonarr_dupes = dupe_result.get("sonarr", {})
                if not radarr_dupes and not sonarr_dupes:
                    logger.info("No duplicates found.")
                else:
                    logger.warning("Duplicate media detected across instances!")
                    for tmdb_id, entries in radarr_dupes.items():
                        if not ENABLE_DUPE_DELETION:
                            logger.warning(f"RADARR duplicate TMDB ID {tmdb_id}:")
                            for e in entries:
                                logger.warning(f"  {e['instance']} :: {e['title']} ({e['year']}) [id={e['internal_id']}]")
                    for tvdb_id, entries in sonarr_dupes.items():
                        logger.warning(f"SONARR duplicate TVDB ID {tvdb_id}:")
                        for e in entries:
                            logger.warning(f"  {e['instance']} :: {e['title']} ({e['year']}) [id={e['internal_id']}]")
            logger.info( f"Cycle completed successfully, sleeping {CIRCLE_TIMER} seconds")
            time.sleep(CIRCLE_TIMER)
        except Exception as e:
            logger.error(
            f"Fatal error in main loop: {e}",
            exc_info=True
            )
        return
			
if __name__ == "__main__":
    main()
