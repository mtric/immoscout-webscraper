from bs4 import BeautifulSoup as bs
import bs4 as bs
import urllib.request
import requests
import time
from datetime import datetime
import pandas as pd
import json
import os

website = "https://www.immobilienscout24.de"
rooms = "2.5"
price = "1400.0"
sorting = "2" # new properties first
searchString = website + "/Suche/de/berlin/berlin/wohnung-mit-balkon-mieten?numberofrooms=" + rooms + "-&price=-" + price + "&geocodes=1276003001048,1276003001046,1276003001054,1276003001017,1276003001034&sorting=" + sorting
filePath = "/Users/ericwalter/Python_Projects/Immobilienscout24/"
max_time = 60 #84600 in sec = 24h
start_time = time.time()
count = 1
while (time.time() - start_time) < max_time:
    print("Loop " + str(count) + " startet.")
    df = pd.DataFrame()
    l = []
    try:
        searchPage = requests.get(searchString)
        searchSoup = bs.BeautifulSoup(searchPage.content, "html.parser")
        for paragraph in searchSoup.find_all("a"):
            if r"/expose/" in str(paragraph.get("href")):
                l.append(paragraph.get("href").split("#")[0])
                l = list(set(l))
        for item in l:
            try:
                itemPage = requests.get(website + item)
                itemSoup = bs.BeautifulSoup(itemPage.content, "html.parser")
                #itemSoup = bs.BeautifulSoup(urllib.request.urlopen('https://www.immobilienscout24.de'+item).read(), 'lxml')
                data = pd.DataFrame(json.loads(str(itemSoup.find_all("script")).split(
                            "keyValues = ")[1].split("}")[0]+str("}")), index=[str(datetime.now())])
                data["URL"] = str(item)
                beschreibung = []

                for i in itemSoup.find_all("pre"):
                    beschreibung.append(i.text)

                data["beschreibung"] = str(beschreibung)
                df = df.append(data)
            except Exception as e:

                print(str(datetime.now())+": " + str(e))
                l = list(filter(lambda x: x != item, l))
                print("ID " + str(item) + " entfernt.")

        df.to_csv(filePath + "Rohdaten/"+str(datetime.now())[:19].replace(":", "").replace(
            ".", "")+".csv", sep=";", decimal=",", encoding="utf-8", index_label="timestamp")

        print("Loop " + str(count) + " endet.")
        count += 1
        time.sleep(60)

    except Exception as e:
        print(str(datetime.now())+": " + str(e))
        time.sleep(60)

print("FERTIG gewebscraped!")

df = pd.DataFrame()

count = 1

for i in os.listdir(filePath + "Rohdaten/"):
    print(str(count)+". Datei: "+str(i))
    count += 1
    df = df.append(pd.read_csv(filePath + "Rohdaten/" +
                               str(i), sep=";", encoding="utf-8", decimal=","))
df.shape
df = df.drop_duplicates(subset="URL")
df.shape
df.to_csv(filePath + "Final.csv",
          sep=";", encoding="utf-8", decimal=",")

print("FERTIG mit CSV Datei!")
