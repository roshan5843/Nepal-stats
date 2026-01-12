import json
import os
import sys
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import pytz

from config import (
    MONGODB_URI,
    DATABASE_NAME,
    COLLECTION_NEPAL_DETAILED,
    IST
)


class NepalMongoDBSync:
    def __init__(self):
        if not MONGODB_URI:
            raise ValueError("MONGODB_URI environment variable not set")

        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DATABASE_NAME]

        print(f"✅ Connected to MongoDB: {DATABASE_NAME}")

    def find_latest_detailed_file(self):
        """Find the most recent Nepal detailed file"""
        print("\n🔍 Searching for latest Nepal detailed file...")

        base_dir = Path("Nepal Boxoffice")

        if not base_dir.exists():
            print(f"❌ Directory not found: {base_dir}")
            return None

        # Find all detailed files
        detailed_files = sorted(base_dir.glob("*_Detailed.json"), reverse=True)

        if not detailed_files:
            print("❌ No Nepal detailed files found")
            return None

        detailed_file = detailed_files[0]
        print(f"✅ Found Detailed: {detailed_file.name}")

        return str(detailed_file)

    def load_json(self, filepath):
        """Load JSON file safely"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data
        except FileNotFoundError:
            print(f"❌ File not found: {filepath}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in {filepath}: {e}")
            return None

    def format_date_code(self, date_str):
        """Convert 2026-01-13 to 20260113"""
        return date_str.replace("-", "")

    def sync_detailed(self, filepath):
        """Sync detailed show data as single date-specific document"""
        data = self.load_json(filepath)
        if not data:
            print("⚠️  No valid detailed data to sync")
            return False

        shows = data.get("shows", [])
        if not shows:
            print("⚠️  No shows in detailed data")
            return False

        date_code = self.format_date_code(data.get("date", ""))
        last_updated = data.get("lastUpdated", "")
        timestamp = datetime.now(IST)

        if not date_code:
            print("❌ No valid date found in data")
            return False

        collection = self.db[COLLECTION_NEPAL_DETAILED]

        print(f"\n📊 Syncing Nepal box office data for {date_code}...")
        print(f"   Shows found: {len(shows)}")

        # Count unique movies
        unique_movies = len(set(show.get("movie_id")
                            for show in shows if show.get("movie_id")))

        # Create single document for this date
        doc = {
            "_id": f"nepal_{date_code}",
            "date": date_code,
            "last_updated": last_updated,
            "synced_at": timestamp,
            "total_shows": len(shows),
            "total_movies": unique_movies,
            "shows": []
        }

        # Add all shows to the shows array
        for show in shows:
            show_doc = {
                "show_id": show.get("show_id"),
                "movie_id": show.get("movie_id"),
                "movie_name": show.get("movie_name"),
                "venue": show.get("venue"),
                "theatre": show.get("theatre"),
                "show_date": show.get("date"),
                "show_time": show.get("time"),
                "seats": show.get("seats", 0),
                "sold": show.get("sold", 0),
                "reserved": show.get("reserved", 0),
                "available": show.get("available", 0),
                "gross": show.get("gross", 0),
                "occupancy_percent": show.get("occupancy_percent", 0)
            }

            # Handle error and skipped flags
            if show.get("error"):
                show_doc["error"] = show.get("error")
            if show.get("skipped"):
                show_doc["skipped"] = True

            doc["shows"].append(show_doc)

        # Upsert the entire date document
        try:
            result = collection.replace_one(
                {"_id": f"nepal_{date_code}"},
                doc,
                upsert=True
            )

            if result.upserted_id:
                print(f"   ✅ Inserted: nepal_{date_code}")
            else:
                print(f"   ✅ Updated: nepal_{date_code}")

            print(f"   📊 Total shows: {len(shows)}")
            print(f"   🎬 Total movies: {unique_movies}")
            return True

        except Exception as e:
            print(f"   ❌ Error syncing data: {e}")
            return False

    def create_indexes(self):
        """Create indexes for better query performance"""
        print("\n🔍 Creating indexes...")

        try:
            collection = self.db[COLLECTION_NEPAL_DETAILED]

            # Date index
            collection.create_index([("date", -1)])

            # Last updated index
            collection.create_index([("last_updated", -1)])

            # Synced at index
            collection.create_index([("synced_at", -1)])

            # Indexes on shows array
            collection.create_index([("shows.movie_id", 1)])
            collection.create_index([("shows.movie_name", 1)])
            collection.create_index([("shows.venue", 1)])
            collection.create_index([("shows.show_date", -1)])
            collection.create_index([("shows.occupancy_percent", -1)])
            collection.create_index([("shows.gross", -1)])

            print(f"✅ Indexes created for {COLLECTION_NEPAL_DETAILED}")
        except Exception as e:
            print(f"⚠️  Index creation warning: {e}")

    def sync_all(self):
        """Sync detailed data only"""
        detailed_file = self.find_latest_detailed_file()

        if not detailed_file:
            print("\n❌ No detailed file found to sync")
            return False

        # Sync detailed
        success = self.sync_detailed(detailed_file)

        # Create indexes
        if success:
            self.create_indexes()

        return success

    def close(self):
        """Close MongoDB connection"""
        self.client.close()
        print("\n👋 MongoDB connection closed")


def main():
    print("🚀 Starting Nepal Box Office MongoDB Sync (Date-Specific Documents)...")
    print(f"⏰ Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")

    syncer = NepalMongoDBSync()

    try:
        success = syncer.sync_all()

        if success:
            print("\n" + "="*60)
            print("✅ NEPAL BOX OFFICE SYNC COMPLETED")
            print("="*60)
            sys.exit(0)
        else:
            print("\n" + "="*60)
            print("❌ SYNC FAILED")
            print("="*60)
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        syncer.close()


if __name__ == "__main__":
    main()
