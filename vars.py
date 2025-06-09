import os
JELLYFIN_API_KEY = os.getenv("JELLYFIN_API_KEY", "")
JELLYFIN_URL = os.getenv("JELLYFIN_URL", "http://jellyfin.lan:8096")
JELLYFIN_USER_ID = os.getenv("JELLYFIN_USER_ID", "")

MDBLIST_API_KEY = os.getenv("MDBLIST_API_KEY", "")

PLEX_TOKEN = os.getenv("PLEX_TOKEN","")
PLEX_URL = os.getenv("PLEX_URL","http://plex.lan:32400")

RADARR_URL = os.getenv("RADARR_URL","http://radarr.lan:7878")
RADARR_API_KEY = os.getenv("RADARR_API_KEY", "")


SONARR_BASE_URL = os.getenv("SONARR_BASE_URL","http://sonarr.lan:8989")
SONARR_API_KEY = os.getenv("SONARR_API_KEY","")

