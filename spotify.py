import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

def init_spotify_client(client_id, client_secret):
    """Initialize Spotify client"""
    credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return spotipy.Spotify(client_credentials_manager=credentials_manager)

def search_spotify_track(spotify_client, track_name, artist_name):
    """Search for a track on Spotify and retrieve its metadata"""
    try:
        results = spotify_client.search(
            q=f'track:{track_name} artist:{artist_name}',
            type='track',
            limit=1
        )
        if results['tracks']['items']:
            track = results['tracks']['items'][0]
            return {
                'spotify_track_id': track['id'],
                'isrc': track['external_ids'].get('isrc', 'N/A'),
                'spotify_track_name': track['name'],
                'spotify_artist_name': track['artists'][0]['name'],
                'album_name': track['album']['name'],
                'release_date': track['album']['release_date']
            }
        return None
    except Exception as e:
        print(f"Error searching Spotify track: {e}")
        return None

def process_spotify_tracks(spotify_client, tracks_data):
    """Process tracks and get Spotify metadata"""
    spotify_results = []
    
    for track in tracks_data.to_dict('records'):
        track_name = track.get('track_name')
        artist_name = track.get('artist_name')
        print(f"\nProcessing track on Spotify: {track_name} by {artist_name}...")
        
        spotify_metadata = search_spotify_track(spotify_client, track_name, artist_name)
        if spotify_metadata:
            spotify_results.append({
                'original_track_name': track_name,
                'original_artist_name': artist_name,
                **spotify_metadata
            })
        else:
            spotify_results.append({
                'original_track_name': track_name,
                'original_artist_name': artist_name
            })
    
    return pd.DataFrame(spotify_results)


# Load credentials
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

# Initialize Spotify client
spotify_client = init_spotify_client(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)

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
    
    # Process tracks through Spotify
    spotify_metadata_df = process_spotify_tracks(spotify_client, tracks_to_search)
    
    # Save Spotify results
    output_file = 'spotify_metadata.csv'
    spotify_metadata_df.to_csv(output_file, index=False)
    print(f"\nSpotify metadata saved to {output_file}")

except Exception as e:
    print(f"Error processing data: {e}")
