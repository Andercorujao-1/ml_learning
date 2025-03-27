import requests
import json

noaaToken = 'uCQtUTXJYjwhyarCNcXdFNWaepfdDJff'

with open("NOAA_GFS/param_list.json", "r") as f:
    params = json.load(f)

params = params['1']
url = 'https://www.ncei.noaa.gov/access/services/search/v1/data'
headers = {'token': noaaToken}



x = requests.get(url,params=params, headers=headers)
if x.status_code == 200:
    print("Request successful!")
    print(x.json())
    with open("NOAA_GFS/data1.json", "w") as f:
        json.dump(x.json(), f)
else:
    print("Error:", x.status_code)
    print(x.content)
