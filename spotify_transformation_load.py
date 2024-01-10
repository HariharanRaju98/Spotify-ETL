import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


def album_fun(data):
    album_list=[]
    for row in data['items']:
        album_id=row['track']['album']['id']
        album_name= row['track']['album']['name']
        album_url=row['track']['album']['external_urls']['spotify']
        album_release_Date=row['track']['album']['release_date']
        album_total_tracks=row['track']['album']['total_tracks']
        album_data={'album_id':album_id,'album_name':album_name,'album_url':album_url,'album_total_tracks':album_total_tracks,'album_release_Date':album_release_Date}
        album_list.append(album_data)
    return album_list

def artist_fun(data):
    artist_list=[]
    for row in data['items']:
        for key, values in row.items():
            if key == 'track':
                for art in (values['artists']):
                    artist_name=art['name']
                    artist_id=art['id']
                    artist_urls=art['external_urls']['spotify']
                    artists={'artist_id':artist_id,'artist_name':artist_name,'external_urls':artist_urls}
                    artist_list.append(artists)
    return artist_list

def song_fun(data):
    song_list=[]
    for row in data['items']:
        song_id=row['track']['id']
        song_name=row['track']['name']
        song_duration=row['track']['duration_ms']
        song_url=row['track']['external_urls']['spotify']
        song_added=row['added_at']
        album_id=row['track']['album']['id']
        artist_id=row['track']['album']['artists'][0]['id']
        song_list.append({"song_id":song_id,"song_name":song_name,"song_duration":song_duration,"song_url":song_url,"song_added":song_added,"album_id":album_id, \
                          "artist_id":artist_id})
    return song_list
                          
                          
def lambda_handler(event, context):
    #boto3 is SDK to communicate with AWS resources
    s3=boto3.client('s3')
    key='spotify-raw-data/to-process/'
    Bucket='spotify-etl-01-03-2024'
    data = s3.list_objects(Bucket=Bucket,Prefix= key)
 
    file_content=[]
    key_content=[]
    
    for row in data['Contents']:
         file_key=row['Key']
         if file_key.split('.')[-1] == 'json':
             
             response =s3.get_object(Bucket=Bucket,Key=file_key)
             content=response['Body']
             json_object=json.loads(content.read())
             
             file_content.append(json_object)
             key_content.append(file_key)
             
    for data in file_content:
        album_list= album_fun(data)
        artist_list =artist_fun(data)
        song_list = song_fun(data)

        album_df=pd.DataFrame(album_list)
        artist_df=pd.DataFrame(artist_list)
        song_df=pd.DataFrame(song_list)
        
        album_df.drop_duplicates(subset=['album_id'])
        artist_df.drop_duplicates(subset=['artist_id'])
        
        album_df['album_release_Date']=pd.to_datetime(album_df['album_release_Date'])
        song_df['song_added']=pd.to_datetime(song_df['song_added'])
       
        song_key =  "spotify-transformed-data/songs_data/song_transformed" + str(datetime.now()) + '.csv'
        song_buffer=StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=song_key,Body=song_content)
        
        
        artist_key="spotify-transformed-data/artist_data/artist_transformed" + str(datetime.now()) + '.csv'
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key,Body=artist_content)
        
        album_key="spotify-transformed-data/album_data/album_tranformed" + str(datetime.now())+ '.csv'
        album_buffer=StringIO()
        album_df.to_csv(album_buffer)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key,Body=album_content)

    s3_resource = boto3.resource('s3')
    for Key in key_content:
        source ={
            'Bucket' :Bucket,
            'Key' : Key     
        }

        s3_resource.meta.client.copy(source,Bucket,'spotify-raw-data/processed/'+ Key.split('/')[-1])
        s3_resource.Object(Bucket, Key).delete()
