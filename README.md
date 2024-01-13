# Spotify-ETL Pipeline

## Introduction
Project Aims in Extracting the top 50 globally played songs in Spotify Application through Spotipy API and transform it in AWS Lambda Environment to desired format and load it to AWS S3.

## Project Flow
  1. Integrating Spotify API amd extracting the data with Python.
  2. Deploying the Extract code in AWS Lambda.
  3. Adding the trigger to run Lambda function on scheduled time.
  4. Deploying the Tranformation and load code in AWS Lambda.
  5. Enabling the Trigger for seemless automation of the pipleine to store the data in S3.
  6. Building Analytics table on top of the semantic data with help of AWS Glue and AWS Athena.
     
## Technology used.
  - Programming Language- Python
  - Amazon web Service(AWS) 
    - Lambda( Lambda code, Lambda Test, Lambda Layer, Lambda Trigger) 
    - S3 (Simple Storage System)
    - Glue Crawler
    - Athena
     
## Dataset/API
  Spotify API- https://developer.spotify.com/documentation/web-api
  
## Packages
  - Pandas
  - Spotipy
    
 

     
