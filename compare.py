import sys
import os
import bz2
import csv
import pandas as pd
import ast
import ujson
import numpy as np
import pathlib
from tqdm import tqdm

directory = "/Volumes/platomo data/Projekte/008 BASt Fussverkehrsaufkommen/Ground Truth/Zählstellen_fertig/"

# directory = "//Users/jonas/Projekte/OTVision/OTVision-macos-v0.2.0.0/Test/"


def events_to_df(filepath, key = 'event_list'):
    # Open otevents-file
    with bz2.open(filepath, "rt", encoding="UTF-8") as file:
        dictfile = ujson.load(file)
    # Convert to DataFrame
    EVENTS = pd.DataFrame.from_dict(dictfile[key])
    return EVENTS

# Import
# GroundTruth
GT_events = pd.DataFrame()
OTA_events = pd.DataFrame()
for root, dirs, files in tqdm(os.walk(directory)):
    for file in files:
        if file.endswith('.otgtevents'):
            path = os.path.join(root, file)
            GT_eventfile = events_to_df(path)
            GT_eventfile['File'] = file
            GT_events = pd.concat([GT_events, GT_eventfile])
        if file.endswith('.otevents'):
            path = os.path.join(root, file)
            OTA_eventfile = events_to_df(path)
            OTA_eventfile['File'] = file
            OTA_events = pd.concat([OTA_events, OTA_eventfile])


print(OTA_events['File'].str.split(pat='_', expand=True))

# Split File Cloumn
OTA_events[['OTCamera', 'fps', 'Date', 'Time', 'Tail']] = OTA_events['File'].str.split(pat='_', expand=True)
GT_events[['OTCamera', 'fps', 'Date', 'Time', 'Tail']] = GT_events['File'].str.split(pat='_', expand=True)
# OTA_events[['OTCamera', 'fps', 'Date', 'Time', 'Tail2']] = OTA_events['video_name'].str.split(pat='_', expand=True)
print(OTA_events.head(2))
# # print(GT_events.head(0))
# # print(OTA_events.head(0))
# del GT_events['File'], GT_events['fps'], GT_events['Tail'], OTA_events['File'],  OTA_events['Ausrichtung'], OTA_events['fps'], OTA_events['Tail']

# Auswertung
OTACounts = pd.DataFrame(OTA_events.groupby(['OTCamera', 'section_id', 'road_user_type', 'Date', 'Time'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTAnalytics_0'})
GTCounts = pd.DataFrame(GT_events.groupby(['OTCamera', 'section_id', 'road_user_type', 'Date', 'Time'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTGroundTruth'})

Counts = pd.merge(OTACounts, GTCounts, left_on=['Date', 'Time', 'section_id', 'road_user_type', 'OTCamera'], right_on = ['Date', 'Time', 'section_id', 'road_user_type', 'OTCamera'])
Counts['Differenz'] = Counts['OTGroundTruth'] - Counts['OTAnalytics_0']
Counts['Diff [%]'] = np.round((1 - np.round(Counts['OTAnalytics_0'] / Counts['OTGroundTruth'], 3)) * 100, 1)

print(Counts)

Counts.to_csv('Compare.csv', index=False)

