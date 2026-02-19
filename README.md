-Tested on Windows   
Mode UPGRADE will skip Sonarr and only do radarr.  
Mode DUPLICATE_CHECK together with ENABLE_DUPE_DELETION will only delete files from Radarr instances, Sonarr will be checked and logged but not deleted.  

How:  
1. Add *arr URLS/API to config file.
2. Make sure the two SKIP_INDIVIDUAL_INSTANES... have one value (true or false) for each instance.  
3. Run.  
  
What it do?  
  1. Circles through all *arr instances with a 60 second delay.  
  1.2. For Radarr it searches for a set amount of Missing Movies.  
  1.3. For Sonarr it searches for a set amount of Missing Seasons.  
     - It will exclude searched Movies/Seasons for 7 days, in case it could not be downloaded/found on the last search.  
  2. It triggers RSS Sync for each instance with a 5 Minute delay between.  
  3. Checks for duplicates. (tmdb-id tvdb-ids that exist in two *arr instance)  
  3.1. If deletion is enabled it deletes the lowest custom scored duplicate from radarr, does nothing for Sonarr.  
  4. It takes a 2500 second (41min) break and starts over.  
     - The comandline window will update and show the last log entry every 15 seconds.  
