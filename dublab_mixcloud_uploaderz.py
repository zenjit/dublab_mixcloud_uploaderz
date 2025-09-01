import os
import json
import csv
import time
import logging
import requests
from pathlib import Path
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# -----------------------
# Logging setup
# -----------------------
log_file = os.path.splitext(os.path.basename(__file__))[0] + ".log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# -----------------------
# Load config
# -----------------------
CONFIG_FILE = "config.json"
if not os.path.exists(CONFIG_FILE):
    raise RuntimeError(f"Config file '{CONFIG_FILE}' not found.")

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = json.load(f)

CLIENT_ID = config.get("client_id")
CLIENT_SECRET = config.get("client_secret")
REDIRECT_URI = config.get("redirect_uri")
TOKEN_FILE = config.get("token_file", "token.txt")
WATCH_FOLDER = config.get("watch_folder", "uploads")
SHOWS_FOLDER = config.get("shows_folder", "shows")
METADATA_FILE = config.get("metadata_file", "shows.csv")

# -----------------------
# Mixcloud Auth
# -----------------------
class MixcloudAuth:
    def __init__(self, client_id, client_secret, redirect_uri, token_file=TOKEN_FILE):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_file = token_file
        self.token = None

    def get_token(self):
        if self.token:
            return self.token
        # Try reading existing token
        if os.path.exists(self.token_file):
            with open(self.token_file, "r", encoding="utf-8") as f:
                self.token = f.read().strip()
        # If no token, run OAuth
        if not self.token:
            self.token = self.run_oauth_flow()
            with open(self.token_file, "w", encoding="utf-8") as f:
                f.write(self.token)
            logging.info(f"Saved new token to {self.token_file}")
        return self.token

    def run_oauth_flow(self):
        """Open browser for OAuth, capture access_token via local HTTP server."""
        auth_url = (
            f"https://www.mixcloud.com/oauth/authorize?"
            f"client_id={self.client_id}&redirect_uri={self.redirect_uri}&response_type=code"
        )
        logging.info(f"Opening browser for Mixcloud authorization...")
        webbrowser.open(auth_url)

        code_holder = {}

        class OAuthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                query = urlparse(self.path).query
                params = parse_qs(query)
                if "code" in params:
                    code_holder["code"] = params["code"][0]
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"Authorization complete! You can close this browser window.")
                else:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(b"Missing code parameter.")

            def log_message(self, format, *args):
                return  # suppress console logging

        # Run temporary HTTP server to capture code
        server_address = ('', 8080)
        httpd = HTTPServer(server_address, OAuthHandler)
        logging.info(f"Waiting for OAuth callback on {self.redirect_uri} ...")
        while "code" not in code_holder:
            httpd.handle_request()

        code = code_holder["code"]
        logging.info(f"Got code: {code}, exchanging for access_token...")

        # Exchange code for token
        token_url = "https://www.mixcloud.com/oauth/access_token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "code": code,
            "grant_type": "authorization_code"
        }
        resp = requests.post(token_url, data=data)
        if resp.status_code != 200:
            logging.error(f"Failed to get access_token: {resp.status_code} {resp.text}")
            raise RuntimeError("OAuth token request failed")
        access_token = resp.json().get("access_token")
        if not access_token:
            raise RuntimeError("No access_token returned by Mixcloud")
        logging.info("OAuth flow completed successfully!")
        return access_token

# -----------------------
# Mixcloud Uploader
# -----------------------
class MixcloudUploader:
    def __init__(self, auth, shows_folder, metadata_file):
        self.auth = auth
        self.shows_folder = shows_folder
        self.metadata_file = metadata_file
        self.metadata = self.load_metadata()

    def load_metadata(self):
        """Load metadata from CSV, splitting tags by ';'."""
        metadata = {}
        if not os.path.exists(self.metadata_file):
            logging.warning(f"Metadata file not found: {self.metadata_file}")
            return metadata

        with open(self.metadata_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                show_name = row.get("show", "").strip()
                if not show_name:
                    logging.info(f"Show not found in shows list")
                    continue
                bio = row.get("bio", "").strip()
                host = row.get("host", "").strip()
                tags_csv = row.get("tags", "")
                tags_list = [t.strip() for t in tags_csv.split(";") if t.strip()]
                metadata[show_name] = {"bio": bio, "tags": tags_list, "host": host,}
        logging.info(f"Loaded metadata for {len(metadata)} shows")
        return metadata

    def upload(self, mp3_path):
        token = self.auth.get_token()
        url = f"https://api.mixcloud.com/upload/?access_token={token}"

        files = {"mp3": open(mp3_path, "rb")}
        filename = os.path.basename(mp3_path)
        show_name_parts = filename.rsplit(" ", 1)
        if len(show_name_parts) == 2:
            show_name, date_part = show_name_parts
            date_part = date_part.replace(".mp3", "")
            try:
                day, month, year = date_part.split(".")
                date_str = f"{year}-{month}-{day}"  # inverted for tracklist URL
                date_str_title = f" {year}.{month}.{day} "  # inverted for tracklist URL
            except Exception as e:
                logging.warning(f"Could not parse date from filename '{filename}': {e}")
                date_str = ""
        else:
            show_name = filename.replace(".mp3", "")
            date_str = ""

        meta = self.metadata.get(show_name, {})
        host = meta.get("host", "")
        bio = meta.get("bio", "")
        tags_list = meta.get("tags", [])[:5]
        show_full_name = show_name + " " + date_str_title + " w/ " + host

        description = bio
        if date_str:
            description += f"\n\nTracklist: http://dublab.cat/shows/{show_name.lower()}/{date_str}"

        picture_path = os.path.join(self.shows_folder, show_name, "picture.jpg")
        if os.path.exists(picture_path):
            files["picture"] = open(picture_path, "rb")

        data = {
            "name": show_full_name,
            "description": description,
            #"hide_stats": "1",
            #"disable_comments": "1"
        }
        for i, tag in enumerate(tags_list):
            data[f"tags-{i}-tag"] = tag

        logging.info(f"Uploading '{mp3_path}' as show '{show_name}'...")
        try:
            resp = requests.post(url, files=files, data=data)
        finally:
            for f in files.values():
                f.close()

        if resp.status_code == 200:
            logging.info("✅ Upload successful")
            self.move_to_show_folder(mp3_path, show_name)
            return True
        elif resp.status_code in (401, 403):
            logging.warning("🔑 Access token invalid or expired — clearing saved token.")
            try:
                os.remove(self.auth.token_file)
            except OSError as e:
                logging.error(f"Failed to remove token file: {e}")
            self.auth.token = None
            return False
        else:
            logging.error(f"❌ Upload failed: {resp.status_code} {resp.text}")
            return False

    def move_to_show_folder(self, mp3_path, show_name):
        """Move MP3 to the show's folder after successful upload."""
        dest_folder = os.path.join(self.shows_folder, show_name)
        os.makedirs(dest_folder, exist_ok=True)
        dest_path = os.path.join(dest_folder, os.path.basename(mp3_path))
        try:
            os.rename(mp3_path, dest_path)
            logging.info(f"Moved MP3 to '{dest_path}'")
        except OSError as e:
            logging.error(f"Failed to move MP3: {e}")

# -----------------------
# Folder Monitor
# -----------------------
class FolderWatcher:
    def __init__(self, watch_folder, uploader, poll_interval=10):
        self.watch_folder = watch_folder
        self.uploader = uploader
        self.poll_interval = poll_interval
        self.seen_files = set()

    def start(self):
        logging.info(f"Watching folder: {self.watch_folder}")
        while True:
            for mp3_file in Path(self.watch_folder).glob("*.mp3"):
                if mp3_file not in self.seen_files:
                    self.seen_files.add(mp3_file)
                    self.uploader.upload(str(mp3_file))
            time.sleep(self.poll_interval)

# -----------------------
# Main
# -----------------------
if __name__ == "__main__":
    auth = MixcloudAuth(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, TOKEN_FILE)
    token = auth.get_token()
    uploader = MixcloudUploader(auth, SHOWS_FOLDER, METADATA_FILE)
    watcher = FolderWatcher(WATCH_FOLDER, uploader)
    watcher.start()
