import sys
import os
import bz2
import csv
import pandas as pd
import ast
import ujson
import numpy as np
import pathlib

# directory = "/Volumes/platomo data/Projekte/008 BASt Fussverkehrsaufkommen/Ground Truth/Zählstellen_fertig/C1/Ost_OTC01/"

directory = "/Volumes/platomo data/Projekte/008 BASt Fussverkehrsaufkommen/Ground Truth/Zählstellen_fertig/C1/Ost_OTC01/"


# def events_to_df(filepath, key = 'event_list'):
#     # Open otevents-file
#     with bz2.open(filepath, "rt", encoding="UTF-8") as file:
#         dictfile = ujson.load(file)
    
#     # Convert to DataFrame
#     EVENTS = pd.DataFrame.from_dict(dictfile[key])

#     return EVENTS


# for roots, dirs, files in os.walk(directory):
#     OTA_eventfiles = []
#     GT_eventfiles = []
#     for file in files:
#         if file.endswith('.otevents'):
#             OTA_eventfiles.append(os.path.join(roots, file))
#         if file.endswith('.otgtevents'):
#             GT_eventfiles.append(os.path.join(roots, file))

#     print(roots)

# for roots in os.walk(directory):
#     OTA_eventfiles = []
#     GT_eventfiles = []
#     for root in roots:
#         for file in root:
#             if file.endswith('.otevents'):
#                 print(file)
#                 print('')
#             if file.endswith('.otgtevents'):
#                 a = 0

        




# Import
def events_to_df(filepath, key = 'event_list'):
    print(filepath)
    # Open otevents-file
    with bz2.open(filepath, "rt", encoding="UTF-8") as file:
        dictfile = ujson.load(file)
    
    # Convert to DataFrame
    EVENTS = pd.DataFrame.from_dict(dictfile[key])

    return EVENTS


# OTAnalytics
OTAEvents = events_to_df(directory + 'C1_Ost_OTC01.events.otevents')
OTAEvents = OTAEvents[OTAEvents['road_user_type'] == 'pedestrian'].reset_index(drop=True)
OTAEvents = OTAEvents[OTAEvents['section_id'].isin(['9','10'])].reset_index(drop=True) # Nur Events mit vergebener section



# GroundTruth




GTEvents1 = events_to_df(directory + 'OTCamera01_FR20_2023-09-19_17-00-00_Sued.otgtevents')
GTEvents2 = events_to_df(directory + 'OTCamera01_FR20_2023-09-19_21-00-00_Sued.otgtevents')
GTEvents3 = events_to_df(directory + 'OTCamera01_FR20_2023-09-20_07-00-00_Sued.otgtevents')
GTEvents4 = events_to_df(directory + 'OTCamera01_FR20_2023-09-20_13-00-00_Sued.otgtevents')

GTEvents = pd.concat([GTEvents1, GTEvents2, GTEvents3, GTEvents4])
del GTEvents1, GTEvents2, GTEvents3, GTEvents4

GTEvents['datetime'] = pd.to_datetime(GTEvents['occurrence'] + 7200, unit='s') #120min fehlen aus irgendeinem Grund

print(GTEvents.head(0))

# Auswertung
OTAEvents['occurrence'] = pd.to_datetime(OTAEvents['occurrence'])
OTAEvents['Zeitspanne'] = OTAEvents['occurrence'].dt.floor('15min')
GTEvents['Zeitspanne'] = GTEvents['datetime'].dt.floor('15min')

OTACounts = pd.DataFrame(OTAEvents.groupby(['Zeitspanne', 'section_id', 'road_user_type'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTAnalytics'})
GTCounts = pd.DataFrame(GTEvents.groupby(['Zeitspanne', 'section_id', 'road_user_type'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTGroundTruth_0'})


Counts = pd.merge(OTACounts, GTCounts, left_on=['Zeitspanne', 'section_id', 'road_user_type'], right_on = ['Zeitspanne', 'section_id', 'road_user_type'])
Counts['Differenz'] = Counts['OTGroundTruth_0'] - Counts['OTAnalytics']
Counts['Diff %'] = (1 - np.round(Counts['OTAnalytics'] / Counts['OTGroundTruth_0'], 3)) * 100

print(Counts)
