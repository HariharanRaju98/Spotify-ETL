import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import os                               
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get("client_secret")
    
    #Save the creditionals in Environmental variables to abstract the sensitive creditionals
    client_cred_manager= SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp= spotipy.Spotify(client_credentials_manager=client_cred_manager)
    
    #link of top 50 globally played songs
    playlist = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_uri=playlist.split('/')[-1]
    
    data=sp.playlist_tracks(playlist_uri)

    #boto3 is AWS SDK to communicate with AWS resources
    client = boto3.client('s3')
    
    filename= "spotify_raw" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket='spotify-etl-01-03-2024',
        Key='spotify-raw-data/to-process/'+ filename,
        Body=json.dumps(data)
        )
