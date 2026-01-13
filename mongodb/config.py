import os
from datetime import datetime, timezone, timedelta

# MongoDB Configuration
MONGODB_URI = os.environ.get("MONGODB_URI")
DATABASE_NAME = os.environ.get("MONGODB_DATABASE", "movie-blog")

# Collection (only detailed)
COLLECTION_NEPAL_DETAILED = "nepal_detailed"
COLLECTION_NEPAL_ADVANCE = "nepal_advance" 

# Timezone
IST = timezone(timedelta(hours=5, minutes=30))

if not MONGODB_URI:
    raise ValueError("❌ MONGODB_URI environment variable not set")
