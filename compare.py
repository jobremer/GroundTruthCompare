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

def sections_to_df(filepath, key = 'sections'):
    # Open otevents-file
    with bz2.open(filepath, "rt", encoding="UTF-8") as file:
        dictfile = ujson.load(file)
    # Convert to DataFrame
    SECTIONS = pd.DataFrame.from_dict(dictfile[key])
    return SECTIONS

# Import
# GroundTruth
GT_events = pd.DataFrame()
OTA_events = pd.DataFrame()
for root, dirs, files in tqdm(os.walk(directory)):
    for file in files:
        if file.endswith('.otgtevents'):
            path = os.path.join(root, file)
            Stelle = pathlib.PurePath(path).parent.parent.name
            GT_eventfile = pd.DataFrame(events_to_df(path))
            GT_eventfile['File'] = file
            GT_eventfile['Messstelle'] = Stelle
     
            # Merge Section names to Events (not working)
            # GT_Sections = sections_to_df(path)
            # GT_eventfile = pd.merge(GT_eventfile, GT_Sections, how='left', left_on='id', right_on='id')
            
            GT_events = pd.concat([GT_events, GT_eventfile])

        if file.endswith('.otevents'):
            path = os.path.join(root, file)
            Stelle = pathlib.PurePath(path).parent.parent.name
            OTA_eventfile = events_to_df(path)
            OTA_eventfile['File'] = file
            OTA_eventfile['Messstelle'] = Stelle
            OTA_events = pd.concat([OTA_events, OTA_eventfile])
            
# Split File Cloumn
OTA_events[['OTCamera', 'fps', 'Date', 'Time', 'Tail']] = OTA_events['File'].str.split(pat='_', expand=True)
GT_events[['OTCamera', 'fps', 'Date', 'Time', 'Tail']] = GT_events['File'].str.split(pat='_', expand=True)

# OTA-Events: Only events that cut two sections
OTA_events = OTA_events[OTA_events['event_type'] == 'section-enter']

OTA_events['road_user_id2'] = OTA_events['Messstelle'] + OTA_events['section_id'] + '_' + OTA_events['Time'] + '_' + OTA_events['road_user_id']
flow = OTA_events[['Messstelle', 'section_id', 'Time', 'road_user_id', 'road_user_id2']].drop_duplicates('road_user_id2')
flow = flow.reset_index(drop=True)



sections = pd.DataFrame(flow.groupby(['Messstelle', 'Time', 'road_user_id'])['section_id'].unique())
sections = sections.rename(columns={'section_id': 'sections'})
sections[['section_1', 'section_2']] = pd.DataFrame(sections['sections'].tolist(), index=sections.index)
# sections.to_csv('sections.csv', sep=';')

flow = pd.merge(flow, sections, on=['Messstelle', 'Time', 'road_user_id'])
# flow.to_csv('flow.csv', sep=';')

del flow['road_user_id2']

OTA_events = pd.merge(OTA_events, flow, how='left', on=['Messstelle', 'Time', 'road_user_id', 'section_id'])




OTA_events[['section_1', 'section_2']] = OTA_events[['section_1', 'section_2']].replace({None: np.NaN})
OTA_events = OTA_events.dropna(subset=['section_1','section_2'])

OTA_events.to_csv('otaevents.csv', sep=';')
GT_events.to_csv('gtevents.csv', sep=';')

# Counts
OTACounts = pd.DataFrame(OTA_events.groupby(['Messstelle', 'OTCamera', 'section_id', 'road_user_type', 'Date', 'Time'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTAnalytics_0'})
GTCounts = pd.DataFrame(GT_events.groupby(['Messstelle', 'OTCamera', 'section_id', 'road_user_type', 'Date', 'Time'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTGroundTruth'})

mergecols = ['Messstelle', 'Date', 'Time', 'section_id', 'road_user_type', 'OTCamera']
Counts = pd.merge(OTACounts, GTCounts, left_on = mergecols, right_on = mergecols)
Counts['Differenz'] = Counts['OTGroundTruth'] - Counts['OTAnalytics_0']
Counts['Diff [%]'] = np.round((1 - np.round(Counts['OTAnalytics_0'] / Counts['OTGroundTruth'], 3)) * 100, 1)




Counts.to_csv(directory + 'Compare_GroundTruth.csv', sep=';', index=False)
print(Counts)