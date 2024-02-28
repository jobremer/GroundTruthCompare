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

# directory = "/Volumes/platomo data/Projekte/008 BASt Fussverkehrsaufkommen/Ground Truth/Zählstellen_fertig/C1/Ost_OTC01/"

directory = "Z:/04_Daten/GroundThruth/Saarbruecken/"

class compare_OTA_GT:
    def events_to_df(filepath, key = 'event_list'):
        # Open otevents-file
        with bz2.open(filepath, "rt", encoding="UTF-8") as file:
            dictfile = ujson.load(file)
        
        # Convert to DataFrame
        EVENTS = pd.DataFrame.from_dict(dictfile[key])

        return EVENTS

    def import_analyze_export(directory):
        # Import
        # GroundTruth
        GT_events = pd.DataFrame()
        OTA_events = pd.DataFrame()
        for root, dirs, files in tqdm(os.walk(directory)):
            for file in files:
                if file.endswith('.otgtevents'):
                    path = os.path.join(root, file)
                    GT_eventfile = compare_OTA_GT.events_to_df(path)
                    GT_eventfile['File'] = file
                    GT_events = pd.concat([GT_events, GT_eventfile])
                if file.endswith('.otevents'):
                    path = os.path.join(root, file)
                    OTA_eventfile = compare_OTA_GT.events_to_df(path)
                    OTA_eventfile['File'] = file
                    OTA_events = pd.concat([OTA_events, OTA_eventfile])

        # Split File Cloumn
        # GT_events[['OTCamera', 'fps', 'Date', 'Time', 'Tail']] = GT_events['File'].str.split(pat='_', expand=True)
        OTA_events[['Name', 'Type', 'Ending']] = OTA_events['File'].str.split(pat='.', expand=True)
        del OTA_events['Type'], OTA_events['Ending']

        # del GT_events['File'], GT_events['fps'], GT_events['Tail'], OTA_events['File'], OTA_events['Position'], OTA_events['Tail'], OTA_events['Tail2'], OTA_events['fps']



        # Häufigkeit der road_user_id
        # enter_leave_events = OTA_events[(OTA_events['event_type'] == 'enter-scene') or (OTA_events['event_type'] == 'leave-scene')]
        enter_events = OTA_events[OTA_events['event_type'] == 'enter-scene'].drop_duplicates('road_user_id')
        leave_events = OTA_events[OTA_events['event_type'] == 'leave-scene'].drop_duplicates('road_user_id')
        section_events = OTA_events[OTA_events['event_type'] == 'section-enter'].drop_duplicates(['road_user_id', 'section_id'])
        section_events = section_events.reset_index(drop=True)

        sections = pd.DataFrame(section_events.groupby(['road_user_id'])['section_id'].unique())
        sections = sections.rename(columns={'section_id': 'sections'})
        sections[['section_1', 'section_2']] = pd.DataFrame(sections['sections'].tolist(), index=sections.index)
        del sections['sections']

        section_events = pd.merge(section_events, sections, how='left', on='road_user_id')
        section_events[['section_1', 'section_2']] = section_events[['section_1', 'section_2']].replace({None: np.NaN})

        OTA_events_filtered = pd.concat([enter_events, section_events], ignore_index=True)
        del enter_events, leave_events, section_events, sections 

        OTA_events_filtered.loc[(OTA_events_filtered['section_1'].isna()) & (OTA_events_filtered['section_2'].isna()), 'event_type2'] = 'No Section'
        OTA_events_filtered.loc[~(OTA_events_filtered['section_1'].isna()) & (OTA_events_filtered['section_2'].isna()), 'event_type2'] = 'Section 1'
        OTA_events_filtered.loc[(OTA_events_filtered['section_1'].isna()) & ~(OTA_events_filtered['section_2'].isna()), 'event_type2'] = 'Section 2'
        OTA_events_filtered.loc[~(OTA_events_filtered['section_1'].isna()) & ~(OTA_events_filtered['section_2'].isna()), 'event_type2'] = 'Flow'

        OTA_events_filtered = OTA_events_filtered[['road_user_id', 'road_user_type', 'frame_number', 'section_id', 'event_type2']]
        # Für road_user_ids, die eine Section schneiden sollen die enter-scene-Events entfernt werden
        temp = OTA_events_filtered[OTA_events_filtered['event_type2'] != 'No Section'].reset_index(drop=True)

        temp['dummy'] = 1
        temp = temp[['road_user_id', 'dummy']].drop_duplicates()
        OTA_events_filtered = pd.merge(OTA_events_filtered, temp, how='left', on='road_user_id')

        OTA_events_filtered = OTA_events_filtered.drop(OTA_events_filtered[(OTA_events_filtered['section_id'].isna()) & (~OTA_events_filtered['dummy'].isna())].index)
        OTA_events_filtered = OTA_events_filtered.sort_values(by=['road_user_id', 'frame_number'])

        OTA_events_filtered = OTA_events_filtered.drop(OTA_events_filtered[OTA_events_filtered.duplicated(subset=['road_user_id'], keep='last')].index)



        GT_events['event_type2'] = 'Flow'

        # # Auswertung
        OTACounts = pd.DataFrame(OTA_events_filtered.groupby(['road_user_type', 'event_type2'])['road_user_id'].count()).reset_index().rename(columns = {'road_user_id': 'OTAnalytics_0'})
        GTCounts = pd.DataFrame(GT_events.groupby(['road_user_type', 'event_type2'])['road_user_id'].count()).reset_index().rename(columns = {'road_user_id': 'OTGroundTruth'})

        Counts = pd.merge(OTACounts, GTCounts, how='outer', left_on=['road_user_type', 'event_type2'], right_on = ['road_user_type', 'event_type2'])
        Counts['Differenz'] = Counts['OTGroundTruth'] - Counts['OTAnalytics_0']
        Counts['Diff [%]'] = np.round((1 - np.round(Counts['OTAnalytics_0'] / Counts['OTGroundTruth'], 3)) * 100, 1)

        Counts.to_csv(directory + 'Counts.csv', sep=';')
        OTA_events_filtered.to_csv(directory + 'OTA_events_filtered5.csv', sep=';')


compare_OTA_GT.import_analyze_export(directory=directory)