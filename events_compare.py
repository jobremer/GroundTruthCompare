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

directory = "Z:/04_Daten/GroundThruth/Saarbruecken"

def events_to_df(directory, key = 'event_list'):
    # Open otevents-file
    with bz2.open(directory, "rt", encoding="UTF-8") as file:
        dictfile = ujson.load(file)
    # Convert to DataFrame
    EVENTS = pd.DataFrame.from_dict(dictfile['event_list'])
    return EVENTS

def sections_to_df(directory, key = 'event_list'):
    # Open otevents-file
    with bz2.open(directory, "rt", encoding="UTF-8") as file:
        dictfile = ujson.load(file)
    # Convert to DataFrame
    SECTIONS = pd.DataFrame.from_dict(dictfile['sections'])
    return SECTIONS

# Import
def events_compare(directory):
    GT_events = pd.DataFrame()
    OTA_events = pd.DataFrame()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.otgtevents'):
                path = os.path.join(root, file)
                GT_eventfile = pd.DataFrame()
                GT_eventfile = events_to_df(path)
                GT_eventfile['File'] = file
                GT_events = pd.concat([GT_events, GT_eventfile])
            if file.endswith('.otevents'):
                path = os.path.join(root, file)
                OTA_eventfile = pd.DataFrame()
                OTA_eventfile = events_to_df(path)
                OTA_Sections = sections_to_df(path)
                OTA_eventfile['File'] = file
                OTA_events = pd.concat([OTA_events, OTA_eventfile])
    
    # Split File Cloumn
    OTA_events[['OTCamera', 'fps', 'Date', 'Tail']] = OTA_events['video_name'].str.split(pat='_', expand=True)
    OTA_events[['Time', 'Tail2']] = OTA_events['Tail'].str.split(pat='.', expand=True)
    
    GT_events[['OTCamera', 'fps', 'Date', 'Time']] = GT_events['video_name'].str.split(pat='_', expand=True)
    GT_events.to_csv('GT_Events.csv', sep=';')
    GT_events = pd.merge(GT_events, OTA_Sections, how='outer', left_on='section_id', right_on='id')
    
    # There are IDs which cut the same section multiple times (something during tracking went wrong)
    # OTA_events = OTA_events.groupby(['road_user_id', 'section_id', 'event_type'])['frame_number'].min()
    # print(OTA_events.head(3))

    ###############################################################################
    # Merge Sections to Events


    OTA_events = pd.merge(OTA_events, OTA_Sections, how='outer', left_on='section_id', right_on='id')
    List_Sections = (OTA_Sections['name']).to_list()
    
    # Tracks identifizieren, die eine section mehrmals schneiden
    OTA_events['event_type2'] = np.where(OTA_events['event_type'] == 'section-enter', 'section_' + OTA_events['name'], OTA_events['event_type'])
    # eventtypecount = pd.DataFrame(OTA_events.groupby(['road_user_id', 'event_type2'])['event_type2'].count()).rename(columns = {'event_type2': 'Event_Type_Count'}).reset_index()
    # eventtypecount = eventtypecount.pivot(index='road_user_id', columns='event_type2', values='Event_Type_Count').rename(columns = {'enter-scene': 'count_enter-scene', 'leave-scene': 'count_leave-scene'}).reset_index().fillna(0)
    # OTA_events = pd.merge(OTA_events, eventtypecount, how='outer', left_on='road_user_id', right_on='road_user_id')

    # # Frame number der Events dazupacken (wide)
    frame_number = OTA_events[['road_user_id', 'frame_number', 'event_type2']]
    frame_number = pd.DataFrame(frame_number.groupby(['road_user_id', 'event_type2'])['frame_number'].min()).reset_index()
    frame_number = frame_number.sort_values(['road_user_id', 'frame_number'], ascending=True).drop_duplicates()
    # print(frame_number.groupby('event_type2')['event_type2'].count())
    frame_number = frame_number.pivot(index='road_user_id', columns='event_type2', values='frame_number').rename(columns = {'enter-scene': 'frame_enter-scene', 'leave-scene': 'frame_leave-scene'}).reset_index().reset_index().fillna(0)
    
    OTA_events = pd.merge(OTA_events, frame_number, how='outer', left_on='road_user_id', right_on='road_user_id')
    OTA_events['name'] = OTA_events['name'].fillna(0)
    
    

    # Jede road_user_id nur einmal um mehrere gleiche Events zu filtern
    OTA_events = OTA_events.drop_duplicates('road_user_id')

    # Counts
    # OTA_events['Trackdauer'] = OTA_events['frame_leave-scene'] - OTA_events['frame_enter-scene']
    
    ## Track-Klassifikation
    # print(OTA_events.dtypes)

    def flowname(Section, OTA_Sections = OTA_Sections, List_Sections=List_Sections):
        if len(OTA_Sections) == 2:
            section1_name = 'section_' + List_Sections[0]
            section2_name ='section_' + List_Sections[1]
            section1 = Section[section1_name]
            section2 = Section[section2_name]
            
            conditions = [
                (section1 != 0) & (section2 != 0) & (section1 <= section2),
                (section1 != 0) & (section2 != 0) & (section1 > section2),
                (section1 != 0) & (section2 == 0),
                (section1 == 0) & (section2 != 0),
                (section1 == 0) & (section2 == 0)
            ]

            choices = [
                'Flow ' + List_Sections[0] + ' -> ' + List_Sections[1],
                'Flow ' + List_Sections[1] + ' -> ' + List_Sections[0],
                'Section ' + List_Sections[0],
                'Section ' + List_Sections[1],
                'Single Event'
                
            ]
        
        # if len(OTA_Sections) == 3:
        #     section1_name = 'section_' + List_Sections[0]
        #     section2_name ='section_' + List_Sections[1]
        #     section3_name ='section_' + List_Sections[2]
        #     section1 = Section[section1_name]
        #     section2 = Section[section2_name]
        #     section3 = Section[section3_name]
            
        #     conditions = [
        #         (section1 != 0) & (section2 != 0) & (section1 <= section2),
        #         (section1 != 0) & (section2 != 0) & (section1 >  section2),
        #         (section1 != 0) & (section3 != 0) & (section1 <= section3),
        #         (section1 != 0) & (section3 != 0) & (section1 >  section3),
        #         (section2 != 0) & (section3 != 0) & (section2 <= section3),
        #         (section2 != 0) & (section3 != 0) & (section2 >  section3),
        #         (section1 != 0) & (section2 == 0) & (section3 == 0),
        #         (section1 == 0) & (section2 != 0) & (section3 == 0),
        #         (section1 == 0) & (section2 == 0) & (section3 != 0),
        #         (section1 == 0) & (section2 == 0) & (section3 == 0)
        #     ]

        #     choices = [
        #         'Flow ' + List_Sections[0] + ' -> ' + List_Sections[1],
        #         'Flow ' + List_Sections[1] + ' -> ' + List_Sections[0],
        #         'Flow ' + List_Sections[0] + ' -> ' + List_Sections[2],
        #         'Flow ' + List_Sections[2] + ' -> ' + List_Sections[0],
        #         'Flow ' + List_Sections[1] + ' -> ' + List_Sections[2],
        #         'Flow ' + List_Sections[2] + ' -> ' + List_Sections[1],
        #         'Section ' + List_Sections[0],
        #         'Section ' + List_Sections[1],
        #         'Section ' + List_Sections[2],
        #         'Single Event'
                
        #     ]
            return conditions, choices
    
    conditions, choices = flowname(Section=OTA_events)
    OTA_events['Klassifikation'] = np.select(conditions, choices, 'Keine Ahnung')
    
    
    if len(OTA_Sections) == 2:
        section1 = List_Sections[0]
        section2 = List_Sections[1]
        
        conditions = [
            GT_events['name'] == section1,
            GT_events['name'] == section2
        ]

        choices = [
            'Flow ' + section1 + ' -> ' + section2,
            'Flow ' + section2 + ' -> ' + section1,          
        ]
    GT_events['Klassifikation'] = np.select(conditions, choices, 'Keine Ahnung')

    
    OTACounts = OTA_events.groupby(['OTCamera', 'Date', 'Time', 'road_user_type', 'Klassifikation'])['road_user_id'].count().reset_index().rename(columns={'road_user_id': 'OTAnalytics_0'})
    GTCounts = pd.DataFrame(GT_events.groupby(['OTCamera', 'road_user_type', 'Date', 'Time', 'Klassifikation'])['road_user_id'].count()).reset_index().rename(columns = {'index': 'classes','road_user_id': 'OTGroundTruth'})
    
    Counts = pd.merge(OTACounts, GTCounts, how='outer', left_on=['Date', 'Time', 'road_user_type', 'OTCamera', 'Klassifikation'], right_on = ['Date', 'Time', 'road_user_type', 'OTCamera', 'Klassifikation'])
    Counts['Differenz'] = Counts['OTGroundTruth'] - Counts['OTAnalytics_0']
    Counts['Diff [%]'] = np.round((1 - np.round(Counts['OTAnalytics_0'] / Counts['OTGroundTruth'], 3)) * 100, 1)
    # ##########################################################################
    # print(Counts)
    print(GT_events)

    return GT_events

# OTA_events, GT_events, Eventcounts = events_compare(directory)


events_compare("Z:/04_Daten/GroundThruth/Saarbruecken")


# Eventcounts.to_csv('Z:/04_Daten/GroundThruth/Saarbruecken/Compare_events.csv', index=False)


