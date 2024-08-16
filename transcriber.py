import whisper
from elasticsearch import Elasticsearch
from datetime import datetime
import requests
import os
import xml.etree.ElementTree as ET
import re

# Initialize Whisper model
model = whisper.load_model("large-v2")

# Initialize Elasticsearch client
es = Elasticsearch([{'host': '128.111.100.50', 'port': 9200, 'scheme': 'http'}])

# Function to extract date from filename
def extract_date_from_filename(filename):
    match = re.search(r'dv_(\d{6})_\d{2}\.mp3', filename)
    if match:
        date_str = match.group(1)
        date_obj = datetime.strptime(date_str, '%m%d%y')
        return date_obj
    return None

# Function to download XML content from a URL
def download_xml_content(xml_url):
    response = requests.get(xml_url)
    if response.status_code == 200:
        xml_content = response.content.decode('utf-8')
        return xml_content
    else:
        raise ValueError(f"Failed to download XML content from {xml_url}. Status code: {response.status_code}")

# Function to preprocess the XML content
def preprocess_xml_content(xml_content):
    xml_content = xml_content.replace("&", "&amp;")
    return xml_content

# Function to parse XML and extract MP3 URLs and metadata
def parse_xml_for_metadata(xml_url):
    try:
        # Download the XML content
        xml_content = download_xml_content(xml_url)
        
        # Preprocess the XML content before parsing
        xml_content = preprocess_xml_content(xml_content)
        
        # Parse the cleaned XML content
        root = ET.fromstring(xml_content)
    except (ET.ParseError, ValueError) as e:
        raise ValueError(f"Failed to parse XML content: {e}")

    episodes = []
    for item in root.findall('./channel/item'):
        enclosure = item.find('enclosure')
        if enclosure is not None:
            mp3_url = enclosure.get('url')
            metadata = {
                'title': item.find('title').text if item.find('title') is not None else None,
                'link': item.find('link').text if item.find('link') is not None else None,
                'pubDate': item.find('pubDate').text if item.find('pubDate') is not None else None,
                'description': item.find('description').text if item.find('description') is not None else None,
                'itunes_subtitle': item.find('itunes:subtitle', namespaces={'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}).text if item.find('itunes:subtitle', namespaces={'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}) is not None else None,
                'itunes_author': item.find('itunes:author', namespaces={'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}).text if item.find('itunes:author', namespaces={'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}) is not None else None,
                'itunes_summary': item.find('itunes:summary', namespaces={'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}).text if item.find('itunes:summary', namespaces={'itunes': 'http://www.itunes.com/dtds/podcast-1.0.dtd'}) is not None else None
            }
            episodes.append({'mp3_url': mp3_url, 'metadata': metadata})
    
    return episodes

# Function to download MP3 file
def download_mp3(mp3_url, download_folder):
    filename = os.path.basename(mp3_url)
    file_path = os.path.join(download_folder, filename)
    
    response = requests.get(mp3_url)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        return file_path
    else:
        print(f"Failed to download {mp3_url}")
        return None

# Folder to store downloaded MP3 files
download_folder = "downloaded_mp3s"
os.makedirs(download_folder, exist_ok=True)

# URL of the XML file
xml_url = "http://slapplebags:Rfs387Ax!!@www.superfreaksideshow.com/members3/wp-content/plugins/s2member-files/s2member-file-inline/s2member-file-remote/2011.xml"

# Parse the XML content from the URL for MP3 URLs and metadata
episodes = parse_xml_for_metadata(xml_url)

# List to track skipped episodes
skipped_episodes = []

# Loop through all episodes
for episode in episodes:
    mp3_url = episode['mp3_url']
    metadata = episode['metadata']
    
    file_path = download_mp3(mp3_url, download_folder)
    
    if file_path:
        filename = os.path.basename(file_path)
        date = extract_date_from_filename(filename)
        
        try:
            result = model.transcribe(file_path, task="transcribe")
            
            # Create a document to insert into Elasticsearch
            doc = {
                'filename': filename,
                'date': date.isoformat() if date else None,
                'transcription': result,  # Store the full transcription result including segments
                'timestamp': datetime.now(),
                'title': metadata['title'],
                'link': metadata['link'],
                'pubDate': metadata['pubDate'],
                'description': metadata['description'],
                'itunes_subtitle': metadata['itunes_subtitle'],
                'itunes_author': metadata['itunes_author'],
                'itunes_summary': metadata['itunes_summary'],
                'transcription_history': [
                    {
                        'text': "No edits yet",  # Placeholder text
                        'timestamp': datetime.now().isoformat(),
                        'edited_by': 'System'
                    }
                ] 
            }

            # Index the document in Elasticsearch
            es.index(index="transcriptions_v2", document=doc)

            print(f"Transcription for {filename} successfully indexed in Elasticsearch")
        
        except Exception as e:
            print(f"Skipping {filename} due to an error during transcription: {e}")
            skipped_episodes.append({
                'filename': filename,
                'reason': f"Transcription error: {e}"
            })
    else:
        print(f"Skipping {filename} due to download failure")
        skipped_episodes.append({
            'filename': filename,
            'reason': "Download failure"
        })

# Print summary of skipped episodes
if skipped_episodes:
    print("\nSummary of Skipped Episodes:")
    for episode in skipped_episodes:
        print(f"Filename: {episode['filename']} - Reason: {episode['reason']}")
else:
    print("\nAll episodes processed successfully without any skips.")
