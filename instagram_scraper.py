import os
import csv
import json
import subprocess
import datetime
import time
import pandas as pd
from urllib.parse import urlparse
import re
from tqdm import tqdm
import uuid

def setup_directories(base_path=None):
    """Create directory structure based on provided base_path or current date."""
    if base_path is None:
        date_str = datetime.datetime.now().strftime("date=%d-%m-%Y")
        base_path = date_str
    
    directories = [
        os.path.join(base_path, "Instagram Post"),
        os.path.join(base_path, "Instagram Reel"),
        os.path.join(base_path, "CSV_Posts"),
        os.path.join(base_path, "CSV_Reels"),
        os.path.join(base_path, "Metadata_Post"),
        os.path.join(base_path, "Metadata_Reels")
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    return base_path

def load_media_ids(media_ids_file):
    """Load previously downloaded media IDs as strings."""
    if os.path.exists(media_ids_file):
        return set(pd.read_csv(media_ids_file, dtype={"media_id": str})["media_id"])
    return set()

def save_media_id(media_ids_file, media_id):
    """Append new media ID to media_ids.csv as a string."""
    with open(media_ids_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if os.path.getsize(media_ids_file) == 0:
            writer.writerow(["media_id"])
        writer.writerow([str(media_id)])

def extract_media_id(url):
    """Extract media ID (shortcode) from Instagram URL."""
    pattern = r"(?:reels?|tv|p)/([A-Za-z0-9_-]+)/?"
    match = re.search(pattern, url)
    return match.group(1) if match else None

def extract_username(input_data):
    """Extract username from Instagram account URL or raw account name."""
    if input_data.startswith(('http://', 'https://')):
        parsed_url = urlparse(input_data)
        path = parsed_url.path.strip('/')
        if path and not path.startswith(('p/', 'reel/', 'reels/', 'tv/')):
            return path
        return None
    if re.match(r'^[A-Za-z0-9._]+$', input_data):
        return input_data
    return None

def download_media(url, output_dir, media_type, account_name=None, expected_extension=None, write_metadata=True, retries=3, delay=5):
    """Download media using gallery-dl with a custom config file, with retries."""
    if account_name:
        output_dir = os.path.join(output_dir, account_name)
    os.makedirs(output_dir, exist_ok=True)
    
    for attempt in range(retries):
        try:
            subprocess.run(["gallery-dl", "--version"], capture_output=True, text=True, check=True)
            cmd = ["gallery-dl", "--config", "gallery-dl.conf", url, "-D", output_dir]
            if write_metadata:
                cmd.append("--write-metadata")
            if media_type == "Reel" and expected_extension:
                cmd.extend(["--filter", f"extension == '{expected_extension}'"])
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            if result.returncode != 0:
                print(f"Error downloading {media_type} from {url} (attempt {attempt + 1}/{retries}): {result.stderr}")
                if attempt < retries - 1:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                continue
            
            # Log gallery-dl output for debugging
            print(f"gallery-dl stdout for {url}: {result.stdout}")
            print(f"gallery-dl stderr for {url}: {result.stderr}")
            
            # Verify downloaded files (relaxed for Posts)
            image_extensions = {'jpg', 'jpeg', 'webp', 'png', 'gif'}
            expected_extensions = {'mp4'} if media_type == "Reel" else image_extensions
            downloaded_files = [f for f in os.listdir(output_dir) if not f.endswith('.json')]  # Accept any non-JSON file for Posts
            if media_type == "Reel":
                downloaded_files = [f for f in downloaded_files if f.lower().endswith('.mp4')]
            if not downloaded_files:
                print(f"No {media_type} files downloaded to {output_dir} for {url}")
                return False, []
            
            # Clean up files that don't match the expected media type
            valid_files = []
            for file in downloaded_files:
                file_ext = os.path.splitext(file)[1][1:].lower()
                if media_type == "Reel" and file_ext not in expected_extensions:
                    try:
                        os.remove(os.path.join(output_dir, file))
                        print(f"Removed incorrect file {file} from {output_dir}")
                    except Exception as e:
                        print(f"Error removing incorrect file {file}: {e}")
                else:
                    valid_files.append(file)
            
            return True, valid_files
        except FileNotFoundError:
            print("Error: gallery-dl is not installed. Please install it using 'pip install gallery-dl'.")
            return False, []
        except subprocess.CalledProcessError as e:
            print(f"Error checking gallery-dl: {e}")
            return False, []
        except subprocess.TimeoutExpired:
            print(f"Timeout downloading {media_type} from {url} (attempt {attempt + 1}/{retries})")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            continue
        except Exception as e:
            print(f"Error downloading {media_type} from {url} (attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            continue
    print(f"Failed to download {media_type} from {url} after {retries} attempts")
    return False, []

def process_metadata(output_dir, media_type, media_id, base_path, account_name=None):
    """Process and append metadata to centralized CSV and JSON files, remove individual JSON files."""
    if account_name:
        output_dir = os.path.join(output_dir, account_name)
    
    # Check if media_id is already processed
    media_ids_file = os.path.join(base_path, "media_ids.csv")
    existing_media_ids = load_media_ids(media_ids_file)
    if media_id in existing_media_ids:
        print(f"Media {media_id} already processed, skipping metadata")
        return
    
    metadata_files = [f for f in os.listdir(output_dir) if f.endswith(".json") and f != "metadata.json"]
    if not metadata_files:
        print(f"No temporary metadata file found in {output_dir}")
        return
    
    # Process only the first metadata file
    metadata_path = os.path.join(output_dir, metadata_files[0])
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception as e:
        print(f"Error reading metadata file {metadata_path}: {e}")
        return
    
    # Centralized CSV and JSON paths
    csv_file = os.path.join(base_path, "CSV_Posts" if media_type == "Post" else "CSV_Reels", "metadata.csv")
    json_file = os.path.join(base_path, "Metadata_Post" if media_type == "Post" else "Metadata_Reels", "metadata.json")
    
    # CSV metadata
    caption = metadata.get("description", metadata.get("caption", ""))
    caption = caption.replace("\n", " ").replace("\r", " ") if caption else ""
    
    csv_data = {
        "media_id": str(media_id),
        "username": metadata.get("owner", {}).get("username", metadata.get("user", {}).get("username", metadata.get("username", ""))),
        "timestamp": metadata.get("date", ""),
        "caption": caption,
        "likes": metadata.get("like_count", metadata.get("likes", 0)),
        "comments": metadata.get("comment_count", metadata.get("comments", 0)),
        "url": metadata.get("post_url", metadata.get("url", ""))
    }
    
    csv_dir = os.path.dirname(csv_file)
    os.makedirs(csv_dir, exist_ok=True)
    try:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_data.keys())
            if os.path.getsize(csv_file) == 0:
                writer.writeheader()
            writer.writerow(csv_data)
    except Exception as e:
        print(f"Error writing to CSV file {csv_file}: {e}")
        return
    
    # Append to centralized JSON file, ensuring no duplicates
    json_dir = os.path.dirname(json_file)
    os.makedirs(json_dir, exist_ok=True)
    existing_metadata = []
    if os.path.exists(json_file):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                existing_metadata = json.load(f)
        except Exception as e:
            print(f"Error reading JSON file {json_file}: {e}")
    
    if not any(m.get("shortcode", m.get("post_shortcode", "")) == media_id for m in existing_metadata):
        existing_metadata.append(metadata)
        try:
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(existing_metadata, f, indent=2)
        except Exception as e:
            print(f"Error writing to JSON file {json_file}: {e}")
    
    # Remove all temporary metadata files
    for metadata_file in metadata_files:
        try:
            os.remove(os.path.join(output_dir, metadata_file))
        except Exception as e:
            print(f"Error deleting temporary metadata file {metadata_file}: {e}")

def remove_duplicate_mp4_files(post_dir, reel_dir, account_name):
    """Remove .mp4 files from Instagram Post directory that are also in Instagram Reel directory."""
    post_account_dir = os.path.join(post_dir, account_name)
    reel_account_dir = os.path.join(reel_dir, account_name)
    
    if not os.path.exists(post_account_dir) or not os.path.exists(reel_account_dir):
        return
    
    reel_files = {f for f in os.listdir(reel_account_dir) if f.lower().endswith('.mp4')}
    
    for file in os.listdir(post_account_dir):
        if file.lower().endswith('.mp4') and file in reel_files:
            try:
                os.remove(os.path.join(post_account_dir, file))
                print(f"Removed duplicate .mp4 file {file} from {post_account_dir}")
            except Exception as e:
                print(f"Error removing duplicate .mp4 file {file}: {e}")

def get_media_info(account_name, media_type="Post", post_range=None, all_posts=False):
    """Fetch media info using gallery-dl without downloading the file."""
    url = f"https://www.instagram.com/{account_name}/" + ("reels/" if media_type == "Reel" else "posts/")
    
    try:
        cmd = ["gallery-dl", "--config", "gallery-dl.conf", "--dump-json", url]
        if post_range and not all_posts:
            cmd.extend(["--range", post_range])
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            print(f"Error fetching {media_type} info: {result.stderr}")
            return None
        
        media_info = json.loads(result.stdout)
        
        if isinstance(media_info, list):
            valid_items = []
            seen_shortcodes = set()
            for item in media_info:
                if isinstance(item, list) and len(item) >= 2:
                    metadata = item[1] if item[0] == 2 and isinstance(item[1], dict) else item[2] if item[0] == 3 and len(item) > 2 and isinstance(item[2], dict) else None
                    if metadata and metadata.get('post_url') and metadata.get('post_shortcode'):
                        shortcode = metadata.get('post_shortcode', metadata.get('shortcode', ''))
                        if shortcode not in seen_shortcodes:
                            valid_items.append(metadata)
                            seen_shortcodes.add(shortcode)
                elif isinstance(item, dict) and item.get('post_url') and item.get('post_shortcode'):
                    shortcode = item.get('post_shortcode', item.get('shortcode', ''))
                    if shortcode not in seen_shortcodes:
                        valid_items.append(item)
                        seen_shortcodes.add(shortcode)
            
            if post_range and not all_posts:
                try:
                    start, end = map(int, post_range.split('-'))
                    valid_items = valid_items[start-1:end]
                except ValueError:
                    print(f"Invalid post_range format: {post_range}")
            return valid_items
        elif isinstance(media_info, dict) and media_info.get('post_url') and media_info.get('post_shortcode'):
            return [media_info]
        else:
            print(f"No valid {media_type} data found in media_info")
            return []
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching {media_type} info for {url}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing JSON output from gallery-dl for {url}: {result.stdout}")
        return None
    except Exception as e:
        print(f"Error fetching {media_type} info for {url}: {e}")
        return None

def scrape_instagram(input_data=None, is_url=True, search=None, post_range=None, all_posts=False, base_path=None):
    """Main function to scrape Instagram media from URLs or accounts."""
    base_path = setup_directories(base_path)
    media_ids_file = os.path.join(base_path, "media_ids.csv")
    downloaded_ids = load_media_ids(media_ids_file)
    
    if is_url:
        if not input_data:
            print("No URL provided for single post/reel scraping")
            return
        
        media_id = extract_media_id(input_data)
        if not media_id:
            print("Invalid Instagram URL")
            return
        
        if media_id in downloaded_ids:
            print(f"Media {media_id} already downloaded")
            return
        
        media_type = "Reel" if "reel" in input_data or "reels" in input_data else "Post"
        output_dir = os.path.join(base_path, f"Instagram {media_type}", media_id)
        os.makedirs(output_dir, exist_ok=True)
        
        expected_extension = "mp4" if media_type == "Reel" else None
        success, _ = download_media(input_data, output_dir, media_type, expected_extension=expected_extension, write_metadata=True, retries=3, delay=5)
        process_metadata(output_dir, media_type, media_id, base_path)  # Always process metadata
        if success:
            save_media_id(media_ids_file, media_id)
            print(f"Successfully downloaded {media_type} with ID {media_id}")
        else:
            print(f"Failed to download {media_type} with ID {media_id}")
    
    else:
        if not search:
            print("No search term provided for account-based scraping")
            return
        
        account_name = extract_username(search)
        if not account_name:
            print("Invalid Instagram account name or URL")
            return
        
        # Process Posts
        post_results = get_media_info(account_name, media_type="Post", post_range=post_range, all_posts=all_posts)
        if not post_results:
            print(f"No posts found for account: {account_name}. Account may be private, empty, or inaccessible.")
        
        downloaded_count = 0
        
        if post_results:
            for item in tqdm(post_results, total=len(post_results), desc=f"Processing posts for {account_name}"):
                if not isinstance(item, dict):
                    print(f"Skipping invalid post item: {item}")
                    continue
                
                media_id = item.get("post_shortcode", item.get("shortcode", ""))
                if not media_id:
                    print(f"No media ID (shortcode) found for post item: {item}")
                    continue
                
                if media_id in downloaded_ids:
                    print(f"Media {media_id} already downloaded")
                    continue
                    
                media_type = "Post"
                expected_extension = None
                
                media_url = item.get("post_url", item.get("url", ""))
                if not media_url:
                    print(f"No media URL found for post item with ID {media_id}")
                    continue
                    
                output_dir = os.path.join(base_path, "Instagram Post")
                
                success, downloaded_files = download_media(media_url, output_dir, media_type, account_name=account_name, expected_extension=expected_extension, retries=3, delay=5)
                process_metadata(output_dir, media_type, media_id, base_path, account_name=account_name)  # Always process metadata
                if success:
                    save_media_id(media_ids_file, media_id)
                    print(f"Successfully downloaded {media_type} with ID {media_id} for account {account_name}. Files: {downloaded_files}")
                    downloaded_count += 1
                else:
                    print(f"Failed to download {media_type} with ID {media_id} for account {account_name}")
                
                # Add delay to avoid rate-limiting
                time.sleep(2)
        
        # Process Reels
        reel_results = get_media_info(account_name, media_type="Reel", post_range=post_range, all_posts=all_posts)
        if not reel_results:
            print(f"No reels found for account: {account_name}. Account may be private, empty, or inaccessible.")
        
        if reel_results:
            for item in tqdm(reel_results, total=len(reel_results), desc=f"Processing reels for {account_name}"):
                if not isinstance(item, dict):
                    print(f"Skipping invalid reel item: {item}")
                    continue
                
                media_id = item.get("post_shortcode", item.get("shortcode", ""))
                if not media_id:
                    print(f"No media ID (shortcode) found for reel item: {item}")
                    continue
                
                if media_id in downloaded_ids:
                    print(f"Media {media_id} already downloaded")
                    continue
                    
                media_type = "Reel"
                expected_extension = "mp4"
                
                media_url = item.get("post_url", item.get("url", ""))
                if not media_url:
                    print(f"No media URL found for reel item with ID {media_id}")
                    continue
                    
                output_dir = os.path.join(base_path, "Instagram Reel")
                
                success, downloaded_files = download_media(media_url, output_dir, media_type, account_name=account_name, expected_extension=expected_extension, retries=3, delay=5)
                process_metadata(output_dir, media_type, media_id, base_path, account_name=account_name)  # Always process metadata
                if success:
                    save_media_id(media_ids_file, media_id)
                    print(f"Successfully downloaded {media_type} with ID {media_id} for account {account_name}. Files: {downloaded_files}")
                    downloaded_count += 1
                else:
                    print(f"Failed to download {media_type} with ID {media_id} for account {account_name}")
                
                # Add delay to avoid rate-limiting
                time.sleep(2)
        
        # Remove duplicate .mp4 files from Instagram Post
        remove_duplicate_mp4_files(
            os.path.join(base_path, "Instagram Post"),
            os.path.join(base_path, "Instagram Reel"),
            account_name
        )
        
        print(f"Downloaded {downloaded_count} items (posts and reels) for account: {account_name}")

if __name__ == "__main__":
    # Example usage
    # Single post/reel URLs
    scrape_instagram(input_data="https://www.instagram.com/reel/DIuxxtjPcnE/", is_url=True)
    scrape_instagram(input_data="https://www.instagram.com/p/DIyBHpaJHvZ/", is_url=True)
    
    # Account-based scraping for all posts and reels
    # scrape_instagram(search="dhwanit.vsit", is_url=False, all_posts=True)
    
    # Account-based scraping with range mode (e.g., posts/reels 1-2)
    scrape_instagram(search="dhwanit.vsit", is_url=False, post_range="1-3")
