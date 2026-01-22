Tested on Windows.
NOTE:
ONLY [WHAT_TO_SEARCH = 'MISSING'] HAS BEEN TESTED YET.
WILL TEST 'UPGRADE' IN A BIT HOPEFULLY.

How:
1. Add *arr URLS/API to config section.
2. Run.

What it do?
- Circles through all *arr instances with a 60 second delay.
- 1. For Radarr it searches for a set amount of Missing Movies.
  2. For Sonarr it searches for a set amount of Missing Seasons.
     - It will exclude searched Movie/Season for 7 days, in case it could not be downloaded/found on the last search.
  3. It triggers RSS Sync for each instance with a 5 Minute delay between.
  4. It takes a 2500 second (41min) break and starts over.
     - The comandline window will update and show the last log entry every 15 seconds.

CONFIGS:
-     # INFO: Disable RSS-Sync-Interval (=0) in Radarr and Sonarr! It will be triggered one instance at a time.
- ENABLE_RSS_CIRCLE = True	# or False
- ENABLE_SONARR = True		# or False
- ENABLE_RADARR = True		# or False
- TIME_BETWEEN_RSS_CALLS = 300	# This is the time between each RSS-Instance, once run it will wait the CIRCLE_TIMER.
- 
- WHAT_TO_SEARCH = "MISSING"	# or UPGRADE
- CIRCLE_TIMER = 2500		# Pause after one full circle through all enabled moduls Radarr-Sonarr-Rss.
- TIME_BETWEEN_ARR_INSTANCES = 60	# Each instance will trigger NUM_MOVIES_TO_UPGRADE with no further break.

- NUM_MOVIES_TO_UPGRADE = 1	# Per Circle this many movie searches will be triggered for each radarr instance.
- MAX_SEASONS = 1			# How many Seasons one circle will Search for each Sonarr instance.
- 
- RECENT_SEARCH_DAYS = 7		# Amount of time when a searched item might be searched again earliest, for full fresh start delete 'searched_movies.json'.
