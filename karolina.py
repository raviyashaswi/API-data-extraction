import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date
import datetime
import sqlite3
#import PIL
from sqlalchemy import text

from PIL import Image,ImageOps
from numpy import asarray
 
#import requests
#from PIL import Image
#import shutil
#import urllib
def imgtonpy(url,i):
    img_url = url
    response = requests.get(img_url)
    if response.status_code:
        i1 = str(i)
        fp = open(i1, 'wb')
        fp.write(response.content)
        #print(response.content)
        fp.close()

    
    # load the image and convert into
    # numpy array
    img = Image.open(i1)
    img = ImageOps.grayscale(img)
    numpydata = asarray(img)
    
# data
    return numpydata

def check_if_valid_data(df:pd.DataFrame):
    #print("got here")
    if df.empty:
        print("No image")
        return False
    if pd.Series(df["image"]).is_unique:
        #print("yes")
        return True
    else:
        raise Exception("Primary key check is violated")
    if df.isnull().values.any():
        raise Exception("Null value found")
    #print("hq")
DATABASE_LOCATION = "sqlite:///rover_images.sqlite"
t = """https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/photos?sol=1000&api_key=DEMO_KEY"""
#USER_ID = 
if __name__ == "__main__":
    #headers = {"Accept":"application/json","Content-Type":"application/json",}

    #import datetime

    today = date.today()
    #print(today)



    q = {'api_key':"fhG5jYrt56NHd1rCrAIXmCpBa4UxAK5Yk95gQil1","earth_date" : today}
    r = requests.get(t,q)
    r = r.json()
    #print(r)
    camid = []
    camtype = []
    imgurl = []
    date_taken = []
    #i=0
    for rover in r["photos"]:
        #roverid.append(r["photos"]["id"])
        #print(rover["id"])
        #if rover["camera"]["id"] == 22:
        date_taken.append(rover["earth_date"])    
        camtype.append(rover["camera"]["name"])
        imgurl.append(rover["img_src"])
        #i = i+1
        
        camid.append(rover["id"])
        #print(rover["id"])
        #print(len(camtype),len(camid),len(imgurl))

    cam_dict = {"cam_type" : camtype,"cam_id" : camid,"image" : imgurl,"date_taken" : date_taken}
    cam_dataframe = pd.DataFrame(cam_dict,columns = ["cam_type","cam_id","image","date_taken",])
    #print(cam_dataframe.head(20))

    if check_if_valid_data(cam_dataframe):
        print("Data is Valid")

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("rover_images.sqlite")
    cursor = conn.cursor()
    sql_query = """CREATE TABLE IF NOT EXISTS rover_images(cam_type text,cam_id bigint,image text,date_taken text,CONSTRAINT primary_key_constraint PRIMARY KEY (image));"""
    #print(sql_query)
    cursor.execute(sql_query)
    try:
        cam_dataframe.to_sql(name="rover_images",con= engine,index = False,if_exists='append')
        print("data inserted")
    except:
        print("data already exists")
    conn.close()
    print("Close database successfully")
    #print(roverid)
    #x=r['FHAZ']
    #print(r['copyright'])
    #print(x)
    #img = requests.get(x)
    #print(x)
