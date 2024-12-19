import os
import googleapiclient.discovery
import pandas as pd

def init_youtube_client(api_key):
    """Initialize YouTube client"""
    return googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

def search_youtube_video(youtube_client, query, youtube_api_keys, current_key_index=0):
    """Search for a video on YouTube and retrieve its metadata"""
    try:
        request = youtube_client.search().list(
            q=query,
            part="snippet",
            maxResults=1,
            type="video"
        )
        response = request.execute()
        
        if response['items']:
            video = response['items'][0]
            snippet = video['snippet']
            return {
                'youtube_video_id': video['id']['videoId'],
                'youtube_title': snippet['title'],
                'youtube_description': snippet.get('description', 'N/A'),
                'youtube_channel_title': snippet['channelTitle'],
                'youtube_publish_date': snippet['publishedAt']
            }
        return None
    except googleapiclient.errors.HttpError as e:
        if "quotaExceeded" in str(e):
            # Rotate to next API key
            new_key_index = (current_key_index + 1) % len(youtube_api_keys)
            print(f"Rotating to API key {new_key_index + 1} of {len(youtube_api_keys)}...")
            new_client = init_youtube_client(youtube_api_keys[new_key_index])
            # Retry with new client
            return search_youtube_video(new_client, query, youtube_api_keys, new_key_index)
        print(f"YouTube API error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def process_youtube_tracks(youtube_client, tracks_data, youtube_api_keys):
    """Process tracks and get YouTube metadata"""
    youtube_results = []
    
    for track in tracks_data.to_dict('records'):
        track_name = track.get('track_name')
        artist_name = track.get('artist_name')
        print(f"\nProcessing track on YouTube: {track_name} by {artist_name}...")
        
        youtube_query = f"{track_name} {artist_name}"
        youtube_metadata = search_youtube_video(youtube_client, youtube_query, youtube_api_keys)
        
        if youtube_metadata:
            youtube_results.append({
                'original_track_name': track_name,
                'original_artist_name': artist_name,
                **youtube_metadata
            })
        else:
            youtube_results.append({
                'original_track_name': track_name,
                'original_artist_name': artist_name
            })
    
    return pd.DataFrame(youtube_results)

# Load credentials
YOUTUBE_API_KEYS = [
    os.getenv('YOUTUBE_API_KEY_1'),
    os.getenv('YOUTUBE_API_KEY_2'),
    os.getenv('YOUTUBE_API_KEY_3')
]

# Initialize YouTube client
youtube_client = init_youtube_client(YOUTUBE_API_KEYS[0])

try:
    # Load and prepare data
    csv_file_path = '[DE TS] Song Catalog Data - Data.csv'
    song_data = pd.read_csv(csv_file_path)
    print("CSV file loaded successfully!")
    
    # Prepare tracks data
    tracks_to_search = song_data.rename(columns={
        'ORIGINAL ARTIST': 'artist_name',
        'SONG TITLE': 'track_name'
    })[['track_name', 'artist_name']].dropna()
    
    print("Formatted track list for search:")
    print(tracks_to_search.head())
    
    # Process tracks through YouTube
    youtube_metadata_df = process_youtube_tracks(youtube_client, tracks_to_search, YOUTUBE_API_KEYS)
    
    # Save YouTube results
    output_file = 'youtube_metadata.csv'
    youtube_metadata_df.to_csv(output_file, index=False)
    print(f"\nYouTube metadata saved to {output_file}")

except Exception as e:
    print(f"Error processing data: {e}")
