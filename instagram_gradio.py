import gradio as gr
import os
import shutil
import zipfile
import sys
from io import StringIO
import uuid
import datetime
from instagram_scraper2 import scrape_instagram, setup_directories, get_media_info
import time

def zip_directory(directory_path):
    """Create a zip file of the given directory and return the zip file path."""
    zip_filename = f"{directory_path}.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, os.path.dirname(directory_path))
                zipf.write(file_path, arcname)
    return zip_filename

def capture_output(func, *args, **kwargs):
    """Capture stdout from a function and yield logs internally."""
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
        result = func(*args, **kwargs)
        output = sys.stdout.getvalue()
        yield result, output
    except Exception as e:
        output = sys.stdout.getvalue()
        yield None, output + f"\nError: {str(e)}"
        raise
    finally:
        sys.stdout = old_stdout

def scrape_url(url, media_type):
    """Scrape a single post or reel by URL and return a zip file."""
    if not url:
        yield None
        return
    
    # Create a unique directory for this scrape
    unique_id = str(uuid.uuid4())
    base_path = f"scrape_{unique_id}"
    os.makedirs(base_path, exist_ok=True)
    
    try:
        for output in capture_output(scrape_instagram, input_data=url, is_url=True, base_path=base_path):
            zip_file, _ = output
            if zip_file is None and os.path.exists(base_path):
                zip_file = zip_directory(base_path)
            yield zip_file
    finally:
        # Clean up the temporary directory
        if os.path.exists(base_path):
            shutil.rmtree(base_path)

def scrape_account(account_name, post_range, all_items, media_type):
    """Scrape posts or reels by account name and return a zip file."""
    if not account_name:
        yield None
        return
    
    if not all_items and not post_range:
        yield None
        return
    
    # Validate post_range
    start = end = None
    if post_range and not all_items:
        try:
            if '-' in post_range:
                start, end = map(int, post_range.split('-'))
                if start < 1 or end < start:
                    yield None
                    return
            else:
                start = end = int(post_range)
                if start < 1:
                    yield None
                    return
        except ValueError:
            yield None
            return
    
    # Create a unique directory for this scrape
    unique_id = str(uuid.uuid4())
    base_path = f"scrape_{unique_id}"
    os.makedirs(base_path, exist_ok=True)
    
    try:
        # Fetch media info for the specified media type only
        media_results = get_media_info(account_name, media_type=media_type, post_range=post_range if not all_items else None, all_posts=all_items)
        if not media_results:
            yield None
            return
        
        # Filter results based on range
        if not all_items and post_range:
            if '-' in post_range:
                media_results = media_results[start-1:end]
            else:
                media_results = media_results[start-1:start]
        
        downloaded_count = 0
        
        for item in media_results:
            if not isinstance(item, dict):
                continue
            
            media_id = item.get("post_shortcode", item.get("shortcode", ""))
            if not media_id:
                continue
            
            media_url = item.get("post_url", item.get("url", ""))
            if not media_url:
                continue
            
            # Run scrape_instagram for this single item
            for output in capture_output(
                scrape_instagram,
                input_data=media_url,
                is_url=True,
                base_path=base_path
            ):
                _, _ = output  # Ignore output, just process
                downloaded_count += 1
            
            time.sleep(2)  # Avoid rate-limiting
        
        if downloaded_count == 0:
            yield None
            return
        
        if os.path.exists(base_path):
            zip_file = zip_directory(base_path)
            yield zip_file
        else:
            yield None
    finally:
        # Clean up the temporary directory
        if os.path.exists(base_path):
            shutil.rmtree(base_path)

def create_interface():
    """Create the Gradio interface."""
    with gr.Blocks(title="Instagram Scraper") as demo:
        gr.Markdown("# Instagram Scraper")
        gr.Markdown("Download Instagram posts or reels by URL or account name. The scraped data (media, CSV, and JSON metadata) will be provided as a zip file.")
        
        with gr.Tabs():
            # Posts Tab
            with gr.Tab("Posts"):
                gr.Markdown("### Download Instagram Posts")
                
                with gr.Group():
                    gr.Markdown("#### Option 1: Download by Post URL")
                    post_url = gr.Textbox(label="Post URL", placeholder="e.g., https://www.instagram.com/p/ABC123/")
                    post_url_button = gr.Button("Download Post by URL")
                
                with gr.Group():
                    gr.Markdown("#### Option 2: Download by Account Name")
                    post_account = gr.Textbox(label="Account Name", placeholder="e.g., username or https://www.instagram.com/username/")
                    post_range = gr.Textbox(label="Post Range (e.g., 1 or 1-5)", placeholder="Leave blank if downloading all posts")
                    post_all = gr.Checkbox(label="Download all posts")
                    post_account_button = gr.Button("Download Posts by Account")
                
                post_output = gr.File(label="Download Zip File")
                
                post_url_button.click(
                    fn=scrape_url,
                    inputs=[post_url, gr.State("Post")],
                    outputs=[post_output]
                )
                post_account_button.click(
                    fn=scrape_account,
                    inputs=[post_account, post_range, post_all, gr.State("Post")],
                    outputs=[post_output]
                )
            
            # Reels Tab
            with gr.Tab("Reels"):
                gr.Markdown("### Download Instagram Reels")
                
                with gr.Group():
                    gr.Markdown("#### Option 1: Download by Reel URL")
                    reel_url = gr.Textbox(label="Reel URL", placeholder="e.g., https://www.instagram.com/reel/DEF456/")
                    reel_url_button = gr.Button("Download Reel by URL")
                
                with gr.Group():
                    gr.Markdown("#### Option 2: Download by Account Name")
                    reel_account = gr.Textbox(label="Account Name", placeholder="e.g., username or https://www.instagram.com/username/")
                    reel_range = gr.Textbox(label="Reel Range (e.g., 1 or 1-5)", placeholder="Leave blank if downloading all reels")
                    reel_all = gr.Checkbox(label="Download all reels")
                    reel_account_button = gr.Button("Download Reels by Account")
                
                reel_output = gr.File(label="Download Zip File")
                
                reel_url_button.click(
                    fn=scrape_url,
                    inputs=[reel_url, gr.State("Reel")],
                    outputs=[reel_output]
                )
                reel_account_button.click(
                    fn=scrape_account,
                    inputs=[reel_account, reel_range, reel_all, gr.State("Reel")],
                    outputs=[reel_output]
                )
        
        gr.Markdown("**Note**: Ensure `gallery-dl` is installed (`pip install gallery-dl`) and a valid `gallery-dl.conf` file is present. The zip file contains only the current scrape’s media files, CSV metadata, and JSON metadata.")

    return demo

if __name__ == "__main__":
    demo = create_interface()
    demo.launch()