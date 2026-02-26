import requests
import random
import logging
import time
import json
import os
from datetime import datetime, timezone
import sys
import threading
import re

config = None

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

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='output.log',
    encoding='utf-8',
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO
)

URL_REGEX = r"^^(https?:\/\/)?[a-zA-Z0-9.-]+(:[0-9]{1,5})?$"
API_KEY_REGEX = r"^^[a-f0-9]{32}$"
WHAT_TO_SEARCH_VALUES = {"MISSING", "UPGRADE"}

def load_config():
    global config
    config_file = "config.json"
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file {config_file} not found.")
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)

def initialize_config():
    global RADARR_URLS, RADARR_API_KEYS, SONARR_URLS, SONARR_API_KEYS
    global ENABLE_RSS, ENABLE_SONARR, ENABLE_RADARR, ENABLE_DUPE_CHECK, ENABLE_DUPE_DELETION
    global WHAT_TO_SEARCH, MAX_MOVIES, MAX_SEASONS, CIRCLE_TIMER, TIME_BETWEEN_RSS_CALLS
    global TIME_BETWEEN_ARR_INSTANCES, RECENT_SEARCH_DAYS
    global SKIP_INDIVIDUAL_INSTANCES_RSS, SKIP_INDIVIDUAL_INSTANCES_SEARCH

    RADARR_URLS = config["RADARR_URLS"]
    RADARR_API_KEYS = config["RADARR_API_KEYS"]
    SONARR_URLS = config["SONARR_URLS"]
    SONARR_API_KEYS = config["SONARR_API_KEYS"]

    ENABLE_RSS = config["ENABLE_RSS"]
    ENABLE_SONARR = config["ENABLE_SONARR"]
    ENABLE_RADARR = config["ENABLE_RADARR"]
    ENABLE_DUPE_CHECK = config["ENABLE_DUPE_CHECK"]
    ENABLE_DUPE_DELETION = config["ENABLE_DUPE_DELETION"]

    WHAT_TO_SEARCH = config["WHAT_TO_SEARCH"]
    MAX_MOVIES = config["MAX_MOVIES"]
    MAX_SEASONS = config["MAX_SEASONS"]
    CIRCLE_TIMER = config["CIRCLE_TIMER"]
    TIME_BETWEEN_RSS_CALLS = config["TIME_BETWEEN_RSS_CALLS"]
    TIME_BETWEEN_ARR_INSTANCES = config["TIME_BETWEEN_ARR_INSTANCES"]
    RECENT_SEARCH_DAYS = config["RECENT_SEARCH_DAYS"]
    SKIP_INDIVIDUAL_INSTANCES_RSS = config["SKIP_INDIVIDUAL_INSTANCES_RSS"]
    SKIP_INDIVIDUAL_INSTANCES_SEARCH = config["SKIP_INDIVIDUAL_INSTANCES_SEARCH"]

    # Validate flags length
    if len(SKIP_INDIVIDUAL_INSTANCES_RSS) != len(RADARR_URLS) + len(SONARR_URLS):
        logger.error("Mismatch between number of instances and number of flags in SKIP_INDIVIDUAL_INSTANCES_RSS.")
        return False
    if len(SKIP_INDIVIDUAL_INSTANCES_SEARCH) != len(RADARR_URLS) + len(SONARR_URLS):
        logger.error("Mismatch between number of instances and number of flags in SKIP_INDIVIDUAL_INSTANCES_SEARCH.")
        return False

    # Validate that the flag values are booleans
    if not all(isinstance(flag, bool) for flag in SKIP_INDIVIDUAL_INSTANCES_RSS):
        logger.error("All values in SKIP_INDIVIDUAL_INSTANCES_RSS should be booleans.")
        return False
    if not all(isinstance(flag, bool) for flag in SKIP_INDIVIDUAL_INSTANCES_SEARCH):
        logger.error("All values in SKIP_INDIVIDUAL_INSTANCES_SEARCH should be booleans.")
        return False
    global INSTANCES
    # Initialize instances
    INSTANCES = [
        {
            "type": "radarr",
            "urls": RADARR_URLS,
            "keys": RADARR_API_KEYS,
            "skip_rss": SKIP_INDIVIDUAL_INSTANCES_RSS[:len(RADARR_URLS)],
            "skip_search_trigger": SKIP_INDIVIDUAL_INSTANCES_SEARCH[:len(RADARR_URLS)],
        },
        {
            "type": "sonarr",
            "urls": SONARR_URLS,
            "keys": SONARR_API_KEYS,
            "skip_rss": SKIP_INDIVIDUAL_INSTANCES_RSS[len(RADARR_URLS):],
            "skip_search_trigger": SKIP_INDIVIDUAL_INSTANCES_SEARCH[len(RADARR_URLS):],
        },
    ]
    return True

def validate_config():
    global config
    if not config:
        logger.error("Config is not loaded.")
        return False
    required_keys = [
        "RADARR_URLS", "RADARR_API_KEYS", "SONARR_URLS", "SONARR_API_KEYS",
        "ENABLE_RSS", "ENABLE_SONARR", "ENABLE_RADARR", "ENABLE_DUPE_CHECK",
        "ENABLE_DUPE_DELETION", "WHAT_TO_SEARCH", "MAX_MOVIES", 
        "MAX_SEASONS", "CIRCLE_TIMER", "TIME_BETWEEN_RSS_CALLS", "TIME_BETWEEN_ARR_INSTANCES", "RECENT_SEARCH_DAYS",
        "SKIP_INDIVIDUAL_INSTANCES_RSS", "SKIP_INDIVIDUAL_INSTANCES_SEARCH"
    ]
    for key in required_keys:
        if key not in config:
            logger.error(f"Missing required config key: {key}")
            return False
    if len(config["SKIP_INDIVIDUAL_INSTANCES_RSS"]) != len(config["RADARR_URLS"]) + len(config["SONARR_URLS"]):
        logger.error("Mismatch between number of flags and number of instances in SKIP_INDIVIDUAL_INSTANCES_RSS.")
        return False
    if len(config["SKIP_INDIVIDUAL_INSTANCES_SEARCH"]) != len(config["RADARR_URLS"]) + len(config["SONARR_URLS"]):
        logger.error("Mismatch between number of flags and number of instances in SKIP_INDIVIDUAL_INSTANCES_SEARCH.")
        return False
    for url in config["RADARR_URLS"]:
        if not re.match(URL_REGEX, url):
            logger.error(f"Invalid RADARR URL format: {url}")
            return False
    for url in config["SONARR_URLS"]:
        if not re.match(URL_REGEX, url):
            logger.error(f"Invalid SONARR URL format: {url}")
            return False
    for api_key in config["RADARR_API_KEYS"]:
        if not re.match(API_KEY_REGEX, str(api_key)):
            logger.error(f"Invalid RADARR API key format: {api_key}")
            return False
    for api_key in config["SONARR_API_KEYS"]:
        if not re.match(API_KEY_REGEX, str(api_key)):
            logger.error(f"Invalid SONARR API key format: {api_key}")
            return False
    boolean_flags = [
        "ENABLE_RSS", "ENABLE_SONARR", "ENABLE_RADARR", 
        "ENABLE_DUPE_CHECK", "ENABLE_DUPE_DELETION"
    ]
    for flag in boolean_flags:
        if not isinstance(config.get(flag), bool):
            logger.error(f"Config key '{flag}' should be a boolean.")
            return False
    if config["WHAT_TO_SEARCH"] not in WHAT_TO_SEARCH_VALUES:
        logger.error(f"Invalid WHAT_TO_SEARCH value: {config['WHAT_TO_SEARCH']}")
        return False
    numeric_fields = [
        "MAX_MOVIES", "MAX_SEASONS", 
        "CIRCLE_TIMER", "TIME_BETWEEN_RSS_CALLS", 
        "TIME_BETWEEN_ARR_INSTANCES", "RECENT_SEARCH_DAYS"
    ]
    for field in numeric_fields:
        if not isinstance(config.get(field), int):
            logger.error(f"Config key '{field}' should be an integer.")
            return False
    if config["MAX_MOVIES"] <= 0:
        logger.error("MAX_MOVIES should be a positive integer.")
        return False
    if config["MAX_SEASONS"] <= 0:
        logger.error("MAX_SEASONS should be a positive integer.")
        return False
    if config["CIRCLE_TIMER"] <= 0:
        logger.error("CIRCLE_TIMER should be a positive integer.")
        return False
    if config["TIME_BETWEEN_RSS_CALLS"] <= 0:
        logger.error("TIME_BETWEEN_RSS_CALLS should be a positive integer.")
        return False
    if config["TIME_BETWEEN_ARR_INSTANCES"] <= 0:
        logger.error("TIME_BETWEEN_ARR_INSTANCES should be a positive integer.")
        return False
    if config["RECENT_SEARCH_DAYS"] <= 0:
        logger.error("RECENT_SEARCH_DAYS should be a positive integer.")
        return False
    return True

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_last_log_entry():
    try:
        with open('output.log', 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return lines[-1].strip() if lines else "No logs yet."
    except Exception as e:
        return f"Could not read log file: {e}"

def print_status():
    last_log = get_last_log_entry()
    last_log_trimmed = last_log[:150] + ("..." if len(last_log) > 150 else "")

    banner = [
        "========================================",
        f"RADARR: {'ENABLED' if ENABLE_RADARR else 'DISABLED'}",
        f"SONARR: {'ENABLED' if ENABLE_SONARR else 'DISABLED'}",
        f"RSS:    {'ENABLED' if ENABLE_RSS else 'DISABLED'}",
        f"DUPE CHECK:       {'ENABLED' if ENABLE_DUPE_CHECK else 'DISABLED'}",
        f"DUPE DELETION:    {'ENABLED' if ENABLE_DUPE_DELETION else 'DISABLED'}",
        "----------------------------------------",
        f"Last log: {last_log_trimmed}",
        "========================================",
    ]

    clear_screen()
    print("\n".join(banner))

def status_loop():
    while True:
        print_status()
        time.sleep(15)

def cleanup_searched_movies():
    searched = load_searched_movies()
    now = time.time()
    for movie_id, timestamp in list(searched.items()):
        if now - timestamp > RECENT_SEARCH_DAYS * 86400:
            del searched[movie_id]
    save_searched_movies(searched)

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
        logger.error("Skipping Sonarr, UPGRADE not supported.")
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
        if len(filtered_movie_ids) < MAX_MOVIES:
            random_keys = filtered_movie_ids
        else: 
            random_keys = random.sample(filtered_movie_ids, k=MAX_MOVIES)
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
    for idx, radarr_url in enumerate(RADARR_URLS):
        if SKIP_INDIVIDUAL_INSTANCES_RSS[idx]:
            logger.info(f"Skipping RSS sync for Radarr instance {radarr_url}")
        else:
            headers = {"X-Api-Key": RADARR_API_KEYS[idx]}
            trigger_rss_sync(radarr_url, headers)
            time.sleep(TIME_BETWEEN_RSS_CALLS)
    for idx, sonarr_url in enumerate(SONARR_URLS):
        if SKIP_INDIVIDUAL_INSTANCES_RSS[len(RADARR_URLS) + idx]:
            logger.info(f"Skipping RSS sync for Sonarr instance {sonarr_url}")
        else:
            headers = {"X-Api-Key": SONARR_API_KEYS[idx]}
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

            # Extracting file info for Radarr
            movie_file = item.get("movieFile") or {}
            file_size = movie_file.get("size", 0)
            
            # Additional Check: Resolution/Quality horizontal ranking
            # Higher resolution (e.g., 2160) gets a higher priority
            quality_res = movie_file.get("quality", {}).get("quality", {}).get("resolution", 0)

            id_map.setdefault(uid, []).append({
                "instance": url,
                "title": item.get("title"),
                "year": item.get("year"),
                "internal_id": item.get("id"),
                "has_file": item.get("hasFile", False),
                "score": item.get("customFormatScore") or 0, # Note: Radarr API uses customFormatScore
                "size": file_size,
                "res": quality_res
            })

    duplicates = {k: v for k, v in id_map.items() if len(v) > 1}

    if ENABLE_DUPE_DELETION and instance_type.lower() == "radarr":
        for uid, entries in duplicates.items():
            # SORTING LOGIC:
            # 1. Presence of file (Don't delete a real file for a placeholder)
            # 2. Custom Score (Highest score stays)
            # 3. Resolution (Tie-breaker for score)
            # 4. Size (Final tie-breaker)

            sorted_entries = sorted(entries, key=lambda x: (
                x["has_file"], 
                x["score"], 
                x["res"], 
                x["size"]
            ))

            lowest = sorted_entries[0]

            if len(sorted_entries) < 2:
                continue

            instance_url = lowest["instance"]
            internal_id = lowest["internal_id"]

            try:
                delete_endpoint = f"movie/{internal_id}"
                logger.info(f"Deleting RADARR duplicate '{lowest['title']}' (Score: {lowest['score']}, Size: {lowest['size']}) from {instance_url}")
                
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

def process_duplicatesOLD(urls, keys, endpoint, id_field, instance_type):
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
    duplicates = {k: v for k, v in id_map.items() if len(v) > 1}
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
            result["sonarr"] = process_duplicatesOLD(
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
    global config
    try:
        load_config()
        initialize_config()
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
            config_retries = 0
            load_config()
            initialize_config()
            if not validate_config():
                logger.warning("Invalid configuration detected. Re-Try in 2.5 Minutes.")
                config_retries = config_retries + 1
                if config_retries > 5:
                    logger.warning("Config File failed validity check 5 times. Check the config.json and start again.")
                    return
                time.sleep(150)
                continue
            cleanup_searched_movies()
            logger.info(f"Starting new cycle with goal: {WHAT_TO_SEARCH}")
            if ENABLE_RADARR:
                for idx, radarr_url in enumerate(RADARR_URLS):
                    if SKIP_INDIVIDUAL_INSTANCES_SEARCH[idx]:
                        logger.info(f"Skipping Radarr search trigger for instance {radarr_url}")
                    else:
                        logger.info(f"Processing Radarr instance: {radarr_url}")
                        process_radarr(radarr_url)
                        time.sleep(TIME_BETWEEN_ARR_INSTANCES)

            if ENABLE_SONARR:
                for idx, sonarr_url in enumerate(SONARR_URLS):
                    if SKIP_INDIVIDUAL_INSTANCES_SEARCH[len(RADARR_URLS) + idx]:
                        logger.info(f"Skipping Sonarr search trigger for instance {sonarr_url}")
                    else:
                        logger.info(f"Processing Sonarr instance: {sonarr_url}")
                        process_sonarr(sonarr_url)
                        time.sleep(TIME_BETWEEN_ARR_INSTANCES)

            if ENABLE_RSS:
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
            logger.error(f"Fatal error in main loop: {e}", exc_info=True)
    return
			
if __name__ == "__main__":
    main()
