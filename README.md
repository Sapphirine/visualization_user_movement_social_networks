# visualization_user_movement_social_networks

There are three files and a folder in here. For more informations, please refer to the Part V of the report.

THe main.py is the main file for the website. To enable the website, run ./bin/python-submit main.py in the spark main resporitory.

The test.py provide the modules for the webpages. There are in total six funcations, 
the get_checkin, get_social, get_pairs, recommend friends, recommend_location, recommend_places_2.
and all of them except the recommend_places_2 are adopted in the website and the demo. 
The recommend_places_2 are runnable, but the result is under expectation.

The templates folder contains the html for the websites.

The upload.py and upload2.py is to upload the raw data to the AWS MySQL.
