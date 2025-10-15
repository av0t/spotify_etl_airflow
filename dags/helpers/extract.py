import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time
import os
from airflow.models import Variable
from helpers.logger_config import setup_dag_run_logger

def setup_spotify_client(client_id, client_secret):
    """Initialize Spotify API client"""
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    )
    return spotipy.Spotify(client_credentials_manager=client_credentials_manager)

def get_tracks_from_search(sp, year=2023, tracks_per_term=200, logger=None):
    """Get tracks using search"""
    all_tracks = []
    
    # Search terms for different genres
    search_terms = [
        f'year:{year} genre:pop',
        f'year:{year} genre:hip-hop', 
        f'year:{year} genre:rock',
        f'year:{year} genre:electronic',
        f'year:{year}'  # General search for the year
    ]
    
    for term in search_terms:
        try:
            print(f"Searching for: {term}")
            
            # Get tracks in batches since API limit is 50 per request
            term_tracks = []
            for offset in range(0, tracks_per_term, 50):
                batch_size = min(50, tracks_per_term - offset)  # Don't exceed tracks_per_term
                
                results = sp.search(q=term, type='track', limit=batch_size, offset=offset, market='US')
                tracks = results['tracks']['items']
                
                # Filter for the specified year
                for track in tracks:
                    if track['album']['release_date'].startswith(str(year)):
                        term_tracks.append(track)
                
                time.sleep(0.1)  # Rate limiting
                
                # Stop if we didn't get a full batch (no more results)
                if len(tracks) < batch_size:
                    break
            
            print(f"  Found {len(term_tracks)} tracks from {year}")
            all_tracks.extend(term_tracks)
            
        except Exception as e:
            print(f"Error searching for {term}: {e}")
            if logger:
                logger.error(f"Error searching for {term}: {e}")
            continue
    
    print(f"Total tracks found across all searches: {len(all_tracks)}")
    if logger:
        logger.info(f"Total tracks found across all searches: {len(all_tracks)}")
    
    # Remove duplicates based on track ID
    unique_tracks = {}
    for track in all_tracks:
        if track and track['id'] not in unique_tracks:
            unique_tracks[track['id']] = track
    
    result = list(unique_tracks.values())
    print(f"Unique tracks after removing duplicates: {len(result)}")
    
    return result

def get_cover_image_url(track):
    """Get cover image URL with fallback to default"""
    try:
        # Try to get the largest image (usually first in the list)
        if track['album']['images'] and len(track['album']['images']) > 0:
            return track['album']['images'][0]['url']
        else:
            # Fallback to default image (tame impala)
            return 'https://i.scdn.co/image/ab67616d0000b2739e1cfc756886ac782e363d79'
    except:
        return 'https://i.scdn.co/image/ab67616d0000b2739e1cfc756886ac782e363d79'

def normalize_release_date(release_date):
    """Convert release date to YYYY-MM-DD format"""
    if len(release_date) == 10:  # Already YYYY-MM-DD
        return release_date
    elif len(release_date) == 7:  # YYYY-MM format
        return f"{release_date}-01"  # Add day as 01
    else:  # YYYY format
        return f"{release_date}-01-01"  # Add month and day as 01

def extract_track_info(track):
    """Extract track information and return as dictionary"""
    try:
        # Basic track info
        track_id = track['id']
        track_name = track['name']
        artists = [artist['name'] for artist in track['artists']]
        artist_names = ', '.join(artists)
        artist_count = len(artists)
        
        # Release date - keep as YYYY-MM-DD
        release_date = normalize_release_date(track['album']['release_date'])
        
        # Duration in milliseconds
        duration_ms = track['duration_ms']
        
        # Popularity
        popularity = track['popularity']
        
        # Cover image with fallback
        cover_image_url = get_cover_image_url(track)
        
        # Album type
        album_type = track['album']['album_type']  # single, album, compilation
        
        # Create row data with id as first column
        row_data = {
            'id': track_id,
            'track_name': track_name,
            'artist(s)_name': artist_names,
            'artist_count': artist_count,
            'release_date': release_date,
            'duration_ms': duration_ms,
            'popularity': popularity,
            'cover_image_url': cover_image_url,
            'album_type': album_type
        }
        
        return row_data
        
    except Exception as e:
        print(f"Error extracting data for track {track.get('name', 'Unknown')}: {e}")
        return None

def create_spotify_dataset(client_id, client_secret, year=2023, tracks_per_term=200, logger=None):
    """Main function to create the dataset"""
    print(f"Starting extraction of tracks from {year}...")
    print(f"Target: {tracks_per_term} tracks per search term")
    
    if logger:
        logger.info(f"Starting extraction of tracks from {year}...")
        logger.info(f"Target: {tracks_per_term} tracks per search term")
    
    # Setup Spotify client
    sp = setup_spotify_client(client_id, client_secret)
    
    # Get tracks using search only
    print(f"Searching for tracks from {year}...")
    tracks = get_tracks_from_search(sp, year=year, tracks_per_term=tracks_per_term, logger=logger)
    print(f"Found {len(tracks)} unique tracks from {year}")
    
    if not tracks:
        print("No tracks found. Please check your API credentials and connection.")
        if logger:
            logger.error("No tracks found. Please check your API credentials and connection.")
        return pd.DataFrame()
    
    # Extract data for each track
    print("Extracting track data...")
    dataset = []
    
    for track in tracks:
        row_data = extract_track_info(track)
        if row_data:
            dataset.append(row_data)
    
    # Create DataFrame
    df = pd.DataFrame(dataset)
    
    print(f"Dataset created with {len(df)} tracks")
    
    return df

def extract_spotify_data(**context):
    """
    Extract task for Airflow - wraps the create_spotify_dataset function
    """
    # Track ETL status and metrics
    etl_run_data = {
        'status': 'failure',  # Default to failure, update to success if all goes well
        'extract_status': 'failure',
        'transform_status': 'failure',
        'load_status': 'failure',
        'tracks_extracted': 0,
        'tracks_loaded': 0,
        'error_message': None
    }

    client_id = Variable.get("CLIENT_ID")
    client_secret = Variable.get("CLIENT_SECRET")
    
    # Get parameters (could also come from Airflow variables)
    year = 2023
    tracks_per_term = 300

    dag_id=context['dag'].dag_id,
    run_id=context['run_id']

    # Get logger for this DAG run
    logger = setup_dag_run_logger(dag_id, run_id)
    
    logger.info(f"\n[{dag_id}_{run_id}] Starting data extraction")

    # Create the extraction directory
    os.makedirs('/tmp/spotify_extracted', exist_ok=True)
    
    # Extract data
    df = create_spotify_dataset(client_id, client_secret, year, tracks_per_term, logger)
    
    # Save to file
    file_path = f'/tmp/spotify_extracted/spotify_data.csv'
    df.to_csv(file_path, index=False)
    
    print(f"Spotify data extracted with {len(df)} rows")
    print(f"Saved to: {file_path}")

    etl_run_data['extract_status'] = 'success'
    etl_run_data['tracks_extracted'] = len(df)
    # XCom Push: Send data to next tasks (it sends to metadata database)
    context['task_instance'].xcom_push(key='file_path', value=file_path)
    context['task_instance'].xcom_push(key='etl_run_data', value=etl_run_data)
    
    return file_path