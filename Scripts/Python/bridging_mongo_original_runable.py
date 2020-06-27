# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 18:07:57 2019

@author: albbl
"""
#%% First Step

import pandas as pd # TO BE ABLE TO USE DATAFRAMES
import re # TO USE REGULAR EXPRESSIONS
import os
import json
from pathlib import Path
import edxmongodbstore as MDB
import datetime
import sys
# import parse_courseStructure_json as courseParse
# import concatenateIMGs_json as concat

#####################################
def checkValidity(response, evenType_flag):
    
    if response is not None:
        
        #response =  response.group(1)
        if type(response) is str and len(response) == 0:
            return False
        else:
            if evenType_flag == False: 
                # If it's stated that the required cheking isn't related to event types   
                return True
            
            elif response.replace('"', '') in toCheck_PROBLEM:              
                return [True, 'problem']
            
            elif response.replace('"', '') in toCheck_VIDEO:
                return [True, 'video']
            
            elif response.replace('"', '') in toCheck_NAVIGATION:               
                return [True, 'navigation']
            
            elif response.replace('"', '') in toCheck_FORUM:
                return [True, 'forum']
            """
            elif response.replace('"', '') in toCheck_ENROLLMENT:
                #print("okokok")
                return [True, 'enrollment']
            """
        
    return False

def getDayEvents(file_name):
    f_events = open(file_name, 'r')
    events = [json.loads(event) for event in f_events]
    f_events.close()
    return events

def processUsers(users_df, course_id, db):
    df_aux= general.copy()
    df_aux.insert(1, 'course_id', course_id)
    df_aux.rename(columns={'id':'user_id'}, inplace=True)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    users_dict= df_aux.to_dict(orient='records')
    db.updateUsers(course_id, users_dict)
    return

def processIndicators(indicators_df, course_id, db):
    df_aux = indicators_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveFinalIndicators(df_dict)
    return

def processSeenProblems(seen_problems_df, course_id, db):
    df_aux = seen_problems_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveSeenProblems(df_dict)
    return

def processSeenVideos(seen_videos_df, course_id, db):
    df_aux = seen_videos_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveSeenVideos(df_dict)
    return

def processCourseStructure(course_struct_df, course_id, db):
    df_aux = course_struct_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_dict = df_aux.to_dict(orient='records')
    db.updateCourseStructure(course_id, df_dict)
    return
    
def processEventTrack(event_track_df, course_id, db):
    df_aux = event_track_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveEventTrack(df_dict)    
    return

def getIndicatorsAggr(db, course_id):
    data = db.getFinalIndicatorsAggrData(course_id)
    return data

def exportIndicatorsAggr(db, course_id, route):
    info = getIndicatorsAggr(db, course_id)
    info_df = pd.DataFrame(info)
    #Reorder to match expected output
    cols_order = ["username","user_id","num_events","num_sessions","video_time","problem_time","nav_time","forum_time","total_time","forum_events","nav_events","problem_events","video_events","consecutive_inactivity_days","connected_days","different_videos","different_problems"]
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'aggrData.csv', encoding='utf-8', index=False)
    return info_df

def getSeenVideosAggr(db, course_id):
    data = db.getSeenVideosAggrData()
    return data

def exportSeenVideos(db, course_id, route):
    info = db.getSeenVideos(course_id)
    info_df = pd.DataFrame(info)
    info_df = info_df.drop(columns=['_id'])
    #Reorder to match expected output
    cols_order = ["user_id","time_day","video_id","video_code"]
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'videoInfo.csv', encoding='utf-8', index=False)
    return info_df
    
def getSeenProblemsAggr(db, course_id):
    data = db.getSeenProblemsAggrData()
    return data

def exportSeenProblems(db, course_id, route):
    info = db.getSeenProblems(course_id)
    info_df = pd.DataFrame(info)
    info_df = info_df.drop(columns=['_id'])
    #Reorder to match expected output
    cols_order = ["user_id","time_day","problem_id","block_info","grade","max_grade","nAttempts"]
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'grades_problemInfo.csv', encoding='utf-8', index=False)
    return info_df

def getEventTrackAggr(db):
    data = db.getEventTrackAggrData()
    return data

def getFinalIndicators(db, course_id):
    data = db.getFinalIndicatorsData(course_id)
    return data

def exportAllIndicators(db, course_id, route):
    info = db.getFinalIndicatorsData(course_id)
    info_df = pd.DataFrame(info)
    #Reorder to match expected output
    cols_order = ["user_id","time_day","num_events","num_sessions","video_time","problem_time","nav_time","forum_time","total_time","forum_events","nav_events","problem_events","video_events","consecutive_inactivity_days","connected_days","different_videos","different_problems"]
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'allIndicators.csv', encoding='utf-8', index=False)
    return info_df

def importCsvToDb(db, db_collection, course_id, csv_route):
    info_df = pd.read_csv(csv_route, encoding='utf-8')
    info_df.insert(0, 'course_id', course_id)
    if 'time_day' in info_df.columns:
        info_df['time_day'] = pd.to_datetime(info_df['time_day'])
    df_dict = info_df.to_dict(orient='records')
    db.importCollection(df_dict, course_id, db_collection)
    return    

def setCourseInfo(db, course_name, course_id):
    db.insertCourseInfo(course_name, course_id)
    return

def importLastExistingData(course_id):
    outputFolders = [f.path for f in os.scandir(rootDir) if f.is_dir()]
    last_date = ""
    for outputFolder in outputFolders:
        data = re.search('.*(\\\\|/)OUTPUT-([0-9\-]*)', outputFolder)
        if data is not None and data.group(2) > last_date:
            last_date = data.group(2)
            last_folder = outputFolder
    
    if last_date != "":
        lastOutputDir = Path(last_folder)
        importCsvToDb(db, "final_indicators", course_id, lastOutputDir / "allIndicators.csv")
        importCsvToDb(db, "seen_videos", course_id, lastOutputDir / "videoInfo.csv")
        importCsvToDb(db, "seen_problems", course_id, lastOutputDir / "grades_problemInfo.csv")
    return

def exportCurrentInactivityDays(db, course_id, route):
    info = db.getCurrentInactivityDays(course_id)
    info_df = pd.DataFrame(info)
    info_df = info_df.astype({'consecutive_inactivity_days': 'int32'})
    info_df.to_csv(route / 'user_current_inactivity_days.csv', encoding='utf-8', index=False)
    return info_df

#####################################

#top = os.getcwd()

##############################################################
## PON AQUÍ EL PATH DE TU CARPETA DE ACCOMP\\REQUIRED_FILES ##
##############################################################
#reqDir = 'C:\\Users\\albbl\\Desktop\\ACCOMP\\REQUIRED_FILES'

##############################################################
## Basado en sistema de carpetas actual                     ##
##############################################################
rootDir = Path('/home/nicolas/ACCOMP')
reqDir = rootDir / 'REQUIRED_FILES'


    

#for here, dirs, files in os.walk(top, topdown=True):
#    if 'Events_ALL' in str(dirs) and 'ACCOMP' in str(here):
#        reqDir = here
#        break

studProfile_file = reqDir / 'profiles.csv'

#Get course_info
#To-Do: GET_COURSE_ID_AUTO
course_id = 'course-v1:UAMx+WebApp+1T2019a'
course_name = 'UAM WebApp 1T 2019A'
db = MDB.EdxMongoStore('localhost', 27017, 'edxmongo')
#db = MDB.EdxMongoStore('150.244.59.136', 27017, 'edxmongo', 'user', 'pwd')
course_info_mdb = db.getCourseInfo(course_id)
if course_info_mdb is False:
    setCourseInfo(db, course_name, course_id)
    course_info_mdb = db.getCourseInfo(course_id)

## First time using MongoDB and having previous data folders?
## Say no more! Put this to True and save your time
importPreviousData = False

if importPreviousData is True:
    importLastExistingData(course_id)
    course_info_mdb = db.getCourseInfo(course_id)    

filesToRead = []
toMatch = re.compile("-events-.*\.txt")
eventsDir = reqDir / 'Events_ALL'
for x in os.walk(eventsDir):

    if(toMatch.search(str(x))):
        filesToRead.append(x)
        
routes = pd.DataFrame( columns = ['path', 'filename'] )

        
last_date = datetime.datetime.min
for x in filesToRead:
    i = 0
    while i < len(x[2]):
        file_date_str = re.search('.*events-([0-9\-]*)-edx.txt', x[2][i]).group(1)
        file_date = datetime.datetime.strptime(file_date_str, '%Y-%m-%d')
        if (file_date > course_info_mdb['last_log_date']):
            routes.at[len(routes), 'path'] = x[0]
            routes.at[len(routes) - 1, 'filename'] = x[2][i]
            if last_date < file_date:
                last_date = file_date
        i = i + 1
        
##########################
if len(routes) == 0:
    print("No new routes, it'll crash")

general = pd.read_csv(studProfile_file, encoding='utf-8')
general = general[general["enrollment_mode"] == "verified"]
processUsers(general, course_id, db)

resultsDir = rootDir / ('OUTPUT' + '-' + last_date.strftime('%Y-%m-%d'))
#resultsDir = resultsDir + '-' + str(dateToUse) + '\\'

if resultsDir.exists() == False: 
    resultsDir.mkdir()

toCheck_VIDEO = [
'hide_transcript',
'edx.video.transcript.hidden',
'load_video',
'edx.video.loaded', # paired with "load_video" (Mobile)
'pause_video',
'edx.video.paused', # paired with "pause_video" (Mobile)
'play_video',
'edx.video.played', # paired with "play_video" (Mobile)
'seek_video',
'edx.video.position.changed', # paired with "seek_video" (Mobile)
'show_transcript',
'edx.video.transcript.shown', # paired with "show_transcript" (Mobile)
'speed_change_video',
'stop_video',
'edx.video.stopped', # paired with "stop_video" (Mobile)
'video_hide_cc_menu',
'video_show_cc_menu'
]

toCheck_PROBLEM = [
'edx.problem.hint.demandhint_displayed',
'edx.problem.hint.feedback_displayed',
'problem_check', #(Browser)
#'problem_check', #(Server)
'problem_check_fail',
'problem_graded',
'problem_rescore',
'problem_rescore_fail',
'problem_reset',
'problem_save',
'problem_show',
'reset_problem',
'reset_problem_fail',
'save_problem_fail',
'save_problem_success',
'showanswer'
]

toCheck_NAVIGATION = [
'page_close',
'seq_goto', 
'seq_next', 
'seq_prev',
'edx.ui.lms.link_clicked',
'edx.ui.lms.outline.selected',
'edx.ui.lms.sequence.next_selected',
'edx.ui.lms.sequence.previous_selected',
'edx.ui.lms.sequence.tab_selected'
]

toCheck_FORUM = [
'edx.forum.comment.created',
'edx.forum.response.created',
'edx.forum.response.voted',
'edx.forum.searched',
'edx.forum.thread.created',
'edx.forum.thread.voted',
'edx.forum.thread.viewed'
]

"""
toCheck_ENROLLMENT = [
'edx.course.enrollment.activated', 
'edx.course.enrollment.deactivated',
'edx.course.enrollment.mode_changed',
'edx.course.enrollment.upgrade.clicked',
'edx.course.enrollment.upgrade.succeeded'        
]
"""

# List of events that sould NOT be taken into consideration when computing activity variables
#notAccountable = ['enrollment']

###################### DEDICATED TO DIFF_VIDEOS AND DIFF_PROBLEMS VARIABLES
seenVideos =  pd.DataFrame(columns = ['user_id', 'time_day', 'video_id', 'video_code'])
vidCtr = 0
seenProblems =  pd.DataFrame(columns = ['user_id', 'time_day', 'problem_id', 'block_info', 'grade', 'max_grade', 'nAttempts'])
probCtr = 0
#enrollmentData = pd.DataFrame(columns = ['user_id', 'time_day', 'mode']) # There may be students on 'audit' mode who switched to 'verified' later on. Both cases should be registered.
#enrCtr = 0
#navigationData = pd.DataFrame(columns = ['user_id', 'unit_id', 'from', 'to']) 
#navCtr = 0
######################


eventTypes = ['navigation', 'problem', 'video', 'forum']

eventTrack = pd.DataFrame( columns = ['user_id', 'event_type', 'type_summary', 'time_day', 'time_hourDetail'] )

daysToConsider = pd.DataFrame( columns = ['time_day'] )

a=datetime.datetime.now()

ctr = 0
i = 0
#testCtr = 0
#routes.to_csv(resultsDir / 'routes.csv', encoding='utf-8', index=False)


while i < len(routes):
          
    print("Read log file #",i+1,'/',len(routes))
    #De momento a pelo, pendiente de incluir el course_id dinamico
    #course_id = ''
    course_id = 'course-v1:UAMx+WebApp+1T2019a'
    
    file_name = routes.at[i,'path']+"/"+routes.at[i,'filename']
    events = getDayEvents(file_name)
    #For debbuging purposes
    #print("Number of events: ", len(events))    
    dateCaptured = False
    for event in events:
        ###################### DEDICATED TO DIFF_VIDEOS AND DIFF_PROBLEMS VARIABLES
        addVid = False
        addProb = False
        addEnr = False
        checkDate_video = -1
        checkDate_problem = -1
        ######################
        if 'user_id' in event and checkValidity(event['user_id'], False) == True:
            userID = event['user_id']
        elif 'context' in event and 'user_id' in event['context']:
            userID = event['context']['user_id']
        else:
            userID = ''

        if len(course_id) == 0 and 'course_id' in event:
            course_id = event['course_id']
        
        ######################################
        if dateCaptured == False:
            
            time = event['time']
            dateCaptured = checkValidity(time, False)
            
            if dateCaptured == True:
                
                time = time.replace('"', '')
                time = time.split('T')
                daysToConsider.at[len(daysToConsider.index), 'time_day'] = time[0]
        ######################################        
        
        if checkValidity(userID, False) == True: 
            
            if str(userID) in str(general['id']) or sum(general['id'] == userID):
                eventType = event['event_type']
                    
                vCheck = checkValidity(eventType, True)
                
                if type(vCheck) != bool: 
                    if vCheck[0] == True: 
                            
                        possibleGrade = -1
                        possibleMaxGrade = -1
                        nAttempts = -1
                        
                        eventType = eventType.replace('"', '')
                        eventTrack.at[ctr, 'user_id'] = userID
                        eventTrack.at[ctr, 'event_type'] = eventType.replace('"', '')
                        eventTrack.at[ctr, 'type_summary'] = vCheck[1]
                        
                        time = event['time']
                            
                        ######## VIDEO, PROBLEM & ENROLLMENT EVENTS' MANAGEMENT (FOR DIFF OCURRENCES CALCULATION)
                        if vCheck[1] == 'video':
                            if (type(event['event']) is dict):                                
                                element = event['event']
                            else:
                                element = json.loads(event['event'])
                                
                            elementID = element['id']
                            elementCode = element['code']
                            if checkValidity(elementCode, False) == True:

                                aux = seenVideos.drop_duplicates(subset=['user_id','video_id','video_code'])
                                aux = aux[aux['user_id'] == userID]    
                                
                                if len(aux[(aux['video_id'] == elementID) & (aux['video_code'] == elementCode)]) == 0 or len(aux) == 0:

                                    seenVideos.at[vidCtr, 'user_id'] = userID
                                    seenVideos.at[vidCtr, 'video_id'] = elementID
                                    seenVideos.at[vidCtr, 'video_code'] = elementCode
                                    
                                    addVid = True
                                    
                                elif len(aux[(aux['video_id'] == elementID) & (aux['video_code'] == elementCode)]) > 0:
                                    
                                    checkDate_video = aux[(aux['video_id'] == elementID) & (aux['video_code'] == elementCode)].index[0]
                            
                        elif vCheck[1] == 'problem':
                            #El tipo event puede venir como string, list o dict
                            if type(event['event']) is dict or type(event['event']) is list or (type(event['event']) is str and (event['event'][0] != '{' or event['event'][-1] != '}')):
                                element = event['event']
                            else:
                                element = json.loads(event['event'])

                            if type(element) is dict:
                                if 'grade' in element:                                
                                    possibleGrade = element['grade']
                                if 'max_grade' in element:                                
                                    possibleMaxGrade = element['max_grade']
                                if 'attempts' in element:                                
                                    nAttempts = element['attempts']

                                                        
                            ########## EXCEPTION CONTROL AND SYNTETIC-FIX
                            if type(element) is not dict:
                                if type(element) is list:
                                    if (element[0] == ''):
                                        elementID = element[0]
                                    else:
                                        elementID = element[0].split('_')[1]
                                else: #str
                                    elementID = element.split('_')[1]                                    
                                if checkValidity(elementID, False) == True:
                                    
                                    elementID = course_id+'+type@problem+block@'+elementID
                                    
                                    aux = seenProblems.drop_duplicates(subset=['user_id','block_info'])
                                    aux = aux[aux['user_id'] == userID]                                   
                                 
                                    resCheck = elementID.split('@')
                                    resCheck = ''.join(resCheck[1:])
                                    if len(aux[aux['block_info'] == resCheck ]) == 0 or len(aux) == 0:
    
                                        seenProblems.at[probCtr, 'user_id'] = userID
                                        seenProblems.at[probCtr, 'problem_id'] = elementID
                                        seenProblems.at[probCtr, 'block_info'] = re.search("@(.*)",elementID).group(1)
                                        
                                        #print("y",user_id, elementID)
                                        #print(possibleGrade)
                                                                                    
                                        if possibleGrade != -1:

                                            seenProblems.at[probCtr, 'grade'] = possibleGrade
                                                               
                                        if possibleMaxGrade != -1:
                                            
                                            seenProblems.at[probCtr, 'max_grade'] = possibleMaxGrade
                                        
                                        if nAttempts != -1:

                                            seenProblems.at[probCtr, 'nAttempts'] = nAttempts
                                            
                                        addProb = True
                                   
                                    elif len(aux[aux['block_info'] == resCheck ]) > 0:
                                    
                                        checkDate_problem = aux[aux['block_info'] == resCheck].index[0]
                                                
                                
                            #############
                            
                            elif checkValidity(element, False) == True:
                                
                                if 'problem_id' in element:
                                    elementID = element['problem_id']
                                elif 'module_id' in element:
                                    elementID = element['module_id']
                                else:
                                    elementID = None

                                if checkValidity(elementID, False) == True:
                                                                                                                   
                                    aux = seenProblems.drop_duplicates(subset=['user_id','block_info'])
                                    aux = aux[aux['user_id'] == userID]    

                                    resCheck = elementID.split('@')
                                    resCheck = ''.join(resCheck[1:])

                                    if len(aux[aux['block_info'] == resCheck ]) == 0 or len(aux) == 0:    
                                        
                                        seenProblems.at[probCtr, 'user_id'] = userID
                                        seenProblems.at[probCtr, 'problem_id'] = elementID
                                        seenProblems.at[probCtr, 'block_info'] = resCheck
                                        
                                        #print("x",user_id, elementID)
                                        #print(possibleGrade)
                                        
                                        if possibleGrade != -1:

                                            seenProblems.at[probCtr, 'grade'] = possibleGrade
                                                              
                                        if possibleMaxGrade != -1:
                                            
                                            seenProblems.at[probCtr, 'max_grade'] = possibleMaxGrade
                                            
                                        if nAttempts != -1:
                                            
                                            seenProblems.at[probCtr, 'nAttempts'] = nAttempts    
                                            
                                        addProb = True
                                   
                                    elif len(aux[aux['block_info'] == resCheck ]) > 0:
                                                                                   
                                        checkDate_problem = aux[aux['block_info'] == resCheck].index[0]

                        ########################################

                        if checkValidity(time, False) == True:
                            #time = time.group(1).replace('"', '')
                            #time = time.split('T')
                            time = time.replace('"', '').split('T')
                            
                            eventTrack.at[ctr, 'time_day'] = time[0]
                            eventTrack.at[ctr, 'time_hourDetail'] = time[1].split('+')[0]
                            
                            if addVid == True:
                                
                                seenVideos.at[vidCtr, 'time_day'] = time[0]
                                vidCtr = vidCtr + 1
                            
                            if addProb == True:
                                
                                seenProblems.at[probCtr, 'time_day'] = time[0]
                                probCtr = probCtr + 1

                            if checkDate_video != -1:
                                
                                if pd.to_datetime(time[0], format='%Y-%m-%d') < pd.to_datetime(seenVideos.at[checkDate_video, 'time_day'], format='%Y-%m-%d'):
                                   
                                    seenVideos.at[checkDate_video, 'time_day'] = time[0]
                                    
                            if checkDate_problem != -1:
                                
                                if pd.to_datetime(time[0], format='%Y-%m-%d') < pd.to_datetime(seenProblems.at[checkDate_problem, 'time_day'], format='%Y-%m-%d'):

                                    seenProblems.at[checkDate_problem, 'time_day'] = time[0]
                                    
                                    if possibleGrade != -1:
                                        seenProblems.at[checkDate_problem, 'grade'] = possibleGrade
                                    
                                    if possibleMaxGrade != -1:
                                            
                                        seenProblems.at[checkDate_problem, 'max_grade'] = possibleMaxGrade
                                        
                                    if nAttempts != -1:
                                            
                                        seenProblems.at[checkDate_problem, 'nAttempts'] = nAttempts

                                if possibleGrade != -1 and pd.isnull(seenProblems.at[checkDate_problem, 'grade']):
                                    seenProblems.at[checkDate_problem, 'grade'] = possibleGrade
                                                                                                  
                                if possibleMaxGrade != -1 and pd.isnull(seenProblems.at[checkDate_problem, 'max_grade']):
                                    seenProblems.at[checkDate_problem, 'max_grade'] = possibleMaxGrade
                                    
                                if nAttempts != -1 and pd.isnull(seenProblems.at[checkDate_problem, 'nAttempts']):
                                    seenProblems.at[checkDate_problem, 'nAttempts'] = nAttempts
                                
                                
                        ctr = ctr + 1
        
        #fp.close()
    i = i + 1

b=datetime.datetime.now()
print('Time for log processing:', b-a)

#%% Second Step

"""
eventTrack.to_csv(resultsDir / 'allEvents.csv', encoding='utf-8', index=False)

daysToConsider = pd.to_datetime(daysToConsider['time_day'], format='%Y-%m-%d')
daysToConsider = daysToConsider.sort_values().tolist()
daysToConsider = pd.DataFrame(daysToConsider)

########################################################################
########################################################################

#Similar a allEvents
#eventTrack = eventTrack.loc[~eventTrack[['type_summary']].isin(notAccountable)['type_summary']]
eventTrack.to_csv(resultsDir / 'allEvents_accountabilityFiltered.csv', encoding='utf-8', index=False)


distinctEvents_perUser = eventTrack.groupby(['user_id', 'type_summary', 'time_day']).size().reset_index(name='Freq')

distinctEvents_perUser.to_csv(resultsDir / 'distinctEvents_perUser.csv', encoding='utf-8', index=False)

events_perUser = pd.DataFrame( columns = ['user_id', 'time_day', 'num_events'] )

i = 0 
j = 0
ctr = 0
uniqueUsers = eventTrack['user_id'].unique()
uniqueDates = eventTrack['time_day'].unique()
while i < len(uniqueUsers):
    j = 0
    while j < len(uniqueDates):    
        toCheck = distinctEvents_perUser.loc[(distinctEvents_perUser['user_id'] == uniqueUsers[i]) & (distinctEvents_perUser['time_day'] == uniqueDates[j])]
        
        events_perUser.at[ctr, 'user_id'] = uniqueUsers[i]
        events_perUser.at[ctr, 'time_day'] = uniqueDates[j]
        events_perUser.at[ctr, 'num_events'] = toCheck['Freq'].sum()
        
        ctr = ctr + 1
        j = j + 1
        
    i = i + 1

events_perUser.to_csv(resultsDir / 'events_User.csv', encoding='utf-8', index=False)

########################################################################
########################################################################

sessions_perUser = pd.DataFrame( columns = ['user_id', 'time_day', 'num_sessions'] )
time_perEventType = pd.DataFrame(0, index = [0], columns = ['user_id', 'time_day', 'video', 'problem', 'navigation', 'forum'] )
time_perEventType['time_day'] = time_perEventType.time_day.apply(str)
time_perEventType['user_id'] = time_perEventType.user_id.apply(str)


i = 0
ctr = 0
while i < len(uniqueUsers):
    j = 0
    while j < len(uniqueDates):
        
        numSessions = 0
        
        toCheck = eventTrack.loc[(eventTrack['user_id'] == uniqueUsers[i]) & (eventTrack['time_day'] == uniqueDates[j])]
        toCheck.sort_values('time_hourDetail', axis=0, ascending=True, inplace=True)
        toCheck = toCheck.reset_index()
        
        len_toCheck = len(toCheck) 
        
        if len_toCheck > 0:
            numSessions = 1
        
        x = ( len_toCheck - 1 )
        while x > 0:
                
            deltaS = pd.to_datetime(toCheck.at[x,'time_hourDetail'] , format="%H:%M:%S.%f") - pd.to_datetime(toCheck.at[(x - 1),'time_hourDetail'] , format="%H:%M:%S.%f")  
            deltaS = deltaS.total_seconds()
            minutes = deltaS / 60
            
            if minutes > 5:
               
                numSessions = numSessions + 1                        
                
            x = x - 1

        ######### SESSION'S DATAFRAME FILL
        sessions_perUser.at[ctr, 'user_id'] = uniqueUsers[i]
        sessions_perUser.at[ctr, 'time_day'] = uniqueDates[j]
        sessions_perUser.at[ctr, 'num_sessions'] = numSessions
        
        #ctr = ctr + 1 
        
        y = 0 
        time_perEventType.loc[ctr] = ['0', '0', 0, 0, 0, 0]
        while y < len(eventTypes):
            
            eventToCheck = eventTypes[y]
            
            toCheck_time = toCheck.loc[toCheck['type_summary'] == eventToCheck]
            toCheck_time.sort_values('time_hourDetail', axis=0, ascending=True, inplace=True)
            toCheck_time = toCheck_time.reset_index(drop=True)
            
            x = ( len(toCheck_time) - 1 )
            while x > 0:
                    
                deltaT = pd.to_datetime(toCheck_time.at[x,'time_hourDetail'] , format="%H:%M:%S.%f") - pd.to_datetime(toCheck_time.at[(x - 1),'time_hourDetail'] , format="%H:%M:%S.%f")  
                deltaT = deltaT.total_seconds()
                deltaT = deltaT / 60
                
                if deltaT <= 5:                
                        
                    time_perEventType.at[ctr, eventToCheck] += deltaT
                                        
                x = x - 1
            
            y = y + 1
        
        ######### TIME PER EVENT-TYPE'S DATAFRAME FILL
        time_perEventType.at[ctr, 'user_id'] = uniqueUsers[i]
        time_perEventType.at[ctr, 'time_day'] = uniqueDates[j]
        
        ctr = ctr + 1
                
        j = j + 1
        
    i = i + 1     
     
    
time_perEventType['total_time'] = time_perEventType.iloc[:,1:].sum(axis=1)    
    
sessions_perUser.to_csv(resultsDir / 'sessions_User.csv', encoding='utf-8', index=False)
time_perEventType.to_csv(resultsDir / 'timeEvent_userDay.csv', encoding='utf-8', index=False)



####################################################
allEvents = pd.read_csv(resultsDir / 'allEvents.csv', encoding='utf-8')

distinctEvents_perUser = pd.read_csv(resultsDir / 'distinctEvents_perUser.csv', encoding='utf-8')
distinctEvents_perUser['time_day'] = pd.to_datetime(distinctEvents_perUser['time_day'], format='%Y-%m-%d')
distinctEvents_perUser['user_id'] = distinctEvents_perUser['user_id'].apply(str)

events_perUser = pd.read_csv( resultsDir / 'events_User.csv', encoding='utf-8')
events_perUser['time_day'] = pd.to_datetime(events_perUser['time_day'], format='%Y-%m-%d')
events_perUser['user_id'] = events_perUser['user_id'].apply(str)

sessions_perUser = pd.read_csv(resultsDir / 'sessions_User.csv', encoding='utf-8')
sessions_perUser['time_day'] = pd.to_datetime(sessions_perUser['time_day'], format='%Y-%m-%d')
sessions_perUser['user_id'] = sessions_perUser['user_id'].apply(str)

time_perEventType = pd.read_csv(resultsDir / 'timeEvent_userDay.csv', encoding='utf-8')
time_perEventType['time_day'] = pd.to_datetime(time_perEventType['time_day'], format='%Y-%m-%d')
time_perEventType['user_id'] = time_perEventType['user_id'].apply(str)


studentsToConsider = general['id'].unique().tolist() #allEvents['user_id'].unique().tolist()
aux = pd.DataFrame(columns = ['user_id'] )

finalDf = pd.DataFrame()

i = 0
while i < len(studentsToConsider):
     
    aux.loc[0] = str(studentsToConsider[i])
    
    fD_iter = pd.concat([aux, daysToConsider], ignore_index=True, axis=1)
    fD_iter = fD_iter.fillna(fD_iter.iloc[0,0])
    
    finalDf = finalDf.append(pd.DataFrame(data = fD_iter))
    
    i = i + 1

finalDf.columns = ['user_id','time_day']

finalDf = pd.merge(finalDf, events_perUser, how = 'left', on = ['user_id','time_day'])
finalDf = pd.merge(finalDf, sessions_perUser, how = 'left', on = ['user_id','time_day'])
finalDf = pd.merge(finalDf, time_perEventType, how = 'left', on = ['user_id','time_day'])

finalDf.columns = ['user_id','time_day','num_events','num_sessions','video_time','problem_time','nav_time','forum_time','total_time']

eventsToConsider = distinctEvents_perUser['type_summary'].unique()

i = 0
while i < len(eventsToConsider):
    
    aux = distinctEvents_perUser[distinctEvents_perUser['type_summary'] == eventsToConsider[i]]    
    aux = aux.drop(['type_summary'], axis=1)
    
    if eventsToConsider[i] == 'navigation':
        
        aux.columns = ['user_id','time_day','nav_events']

    elif eventsToConsider[i] == 'forum':
            
        aux.columns = ['user_id','time_day','forum_events']

    elif eventsToConsider[i] == 'problem':
    
        aux.columns = ['user_id','time_day','problem_events']

    elif eventsToConsider[i] == 'video':
    
        aux.columns = ['user_id','time_day','video_events']

    finalDf = pd.merge(finalDf, aux, how='left', on = ['user_id','time_day'])
    
    i = i + 1

finalDf = finalDf.fillna(0)

########## GET CONSECUTIVE INACTIVITY DAYS + DAYS CONNECTED
i = 0
while i < len(studentsToConsider):
    
    aux = finalDf[finalDf['user_id'] == str(studentsToConsider[i])]
    aux2 = aux.iloc[:,2:13]
    
    logiCheck = (aux2 != 0)
    logiCheck = logiCheck.sum(1)
        
    j = 0
    #inact_ctr = 0
    #conn_ctr = 0
    inact_ctr, conn_ctr = db.getUserLastActivityInfo(studentsToConsider[i], course_id)
    while j < len(logiCheck):
        
        if logiCheck.iloc[j] == 0:
            
            inact_ctr += 1
        else:
            
            inact_ctr = 0
            conn_ctr +=1
            
        finalDf.at[logiCheck.index[j], 'consecutive_inactivity_days'] = inact_ctr
        finalDf.at[logiCheck.index[j], 'connected_days'] = conn_ctr

        j = j + 1 
    
    i = i + 1


########## SET NUM DIFF VIDEOS + PROBLEMS
i = 0
while i < len(finalDf['user_id'].unique()):
    
    aux = finalDf[finalDf['user_id'] == str(studentsToConsider[i])]
    aux = aux.iloc[:,0:2]
     
    ####### DIFF VIDEOS' SETTING
    aux2 = seenVideos[seenVideos['user_id'] == int(aux.iloc[0]['user_id'])]
    aux2 = aux2.groupby(['user_id', 'time_day']).size().reset_index(name='Freq')

    j = 0
    j_aux = 0
    elemCtrVid, elemCtrPro = db.getUserLastSeenInfo(studentsToConsider[i], course_id)
    while j < len(aux):

        if j_aux < len(aux2) and aux2.at[j_aux,'time_day'] == str(aux.iloc[j,1]).split(" ")[0]:
            
            elemCtrVid = elemCtrVid + aux2.at[j_aux,'Freq']
            j_aux += 1
            
        finalDf.at[aux.index[j], 'different_videos'] = elemCtrVid

        j = j + 1 
    
    ####### DIFF PROBLEMS' SETTING
    aux2 = seenProblems[seenProblems['user_id'] == int(aux.iloc[0]['user_id'])]
    aux2 = aux2.groupby(['user_id', 'time_day']).size().reset_index(name='Freq')

    j = 0
    j_aux = 0
    while j < len(aux):
                
        if j_aux < len(aux2) and aux2.at[j_aux,'time_day'] == str(aux.iloc[j,1]).split(" ")[0]:
            
            elemCtrPro = elemCtrPro + aux2.at[j_aux,'Freq']
            j_aux += 1
            
        finalDf.at[aux.index[j], 'different_problems'] = elemCtrPro

        j = j + 1 
    
    i = i + 1

#finalDf.to_csv(resultsDir / 'allIndicators.csv', encoding='utf-8', index=False)
processIndicators(finalDf, course_id, db)


################ AGGREGATED DATA
final_aggrData = pd.DataFrame(columns = finalDf.columns.drop('time_day'))

i = 0 
ctr = 0
while i < len(studentsToConsider):
    
    aux = finalDf[finalDf['user_id'] == str(studentsToConsider[i])]
    
    j = 0
    while j < len(aux.columns):
        
        if aux.columns[j] != 'time_day':
            
            if aux.columns[j] == 'different_videos' and aux.columns[j] == 'different_problems': 
                
                final_aggrData.at[ctr, aux.columns[j]] = aux[aux.columns[j]].sum()
            else:
                
                final_aggrData.at[ctr, aux.columns[j]] = aux[aux.columns[j]].max()
    
        j = j + 1

    
    final_aggrData.at[ctr, 'user_id'] = str(studentsToConsider[i])
    ctr = ctr + 1
    
    i = i + 1 
    
######################################################
# Get usernames for final "user_id"s
new_columnNames = general.columns.values
new_columnNames[0] = 'user_id'
general.columns = new_columnNames
general['user_id'] = general.user_id.astype(str)

final_aggrData = pd.merge(final_aggrData, general[['user_id','username']], how='left', on = 'user_id')

# Re-arrange columns so that "username" is the first one
cols = final_aggrData.columns.tolist()
cols = cols[-1:] + cols[:-1]

final_aggrData = final_aggrData[cols]  
######################################################



#Relevant info To Mongo
processSeenProblems(seenProblems, course_id, db)
processSeenVideos(seenVideos, course_id, db)

finalAggrData = exportIndicatorsAggr(db, course_id, resultsDir)
finalSeenProblems = exportSeenVideos(db, course_id, resultsDir)
finalSeenVideos = exportSeenProblems(db, course_id, resultsDir)
finalAllIndicators = exportAllIndicators(db, course_id, resultsDir)
currentInactivityDays = exportCurrentInactivityDays(db, course_id, reqDir)
#final_aggrData.to_csv(resultsDir / 'aggrData.csv', encoding='utf-8', index=False)
#seenProblems.to_csv(resultsDir / 'grades_problemInfo.csv', encoding='utf-8', index=False)
#seenVideos.to_csv(resultsDir / 'videoInfo.csv', encoding='utf-8', index=False)


#enrollmentData.to_csv(resultsDir / 'enrollment.csv', encoding='utf-8', index=False)

os.system('python parse_courseStructure.py') # Should have an argument referred to the course_str's file-path

c=datetime.datetime.now()
print('Rest of the time:' , c-b)

####################
####################


courseDir = rootDir / 'Scripts' / 'Python'
#sys.path.append(reqDir.replace('REQUIRED_FILES', 'Scripts\\Python'))
sys.path.append(courseDir)

parseItem = courseParse.courseStrExtractor(reqDir)

courseStructure = parseItem.strParser(resultsDir)
processCourseStructure(courseStructure, course_id, db)

################################
# IMGS CONCAT TEST NEW VERSION #
# Creates struct in output dir #
################################


emailResourcesTemplateDir = reqDir / 'ACCOMP_VISUAL'
emailResourcesResultsDir = resultsDir / 'ACCOMP_VISUAL'

concatItem = concat.imgConcatenator(reqDir, emailResourcesResultsDir, emailResourcesTemplateDir)

#%% Third Step

###########################
# EJECUTA LOS SCRIPTS DE R
###########################

#r_home = '"'+os.environ['R_HOME']+'\\bin\\Rscript.exe"'
# To be able to write a .bat file with Rscript based on this route

#import subprocess

#rScript_dir = reqDir.replace('REQUIRED_FILES', 'Scripts\\R\\')

#infile=open(reqDir + '\\testBat.bat', "w") #Opens the file
#infile.write(r_home + ' ' + rScript_dir + 'problemGrade_Adequation.R') #Writes the desired contents to the file
#infile.close() #Closes the file

#subprocess.call([reqDir + '\\testBat.bat'])
#os.remove(reqDir + '\\testBat.bat')

#############################

##########################################
# ONCE IMAGES ARE CREATED, CREATE EMAILS #
##########################################

##With False, it generates the extraTimeS
concatItem.createEmails()


####################
# EJECUTA SEND.R
####################

#infile=open(reqDir + '\\testBat.bat2', "w")#Opens the file
#infile.write(r_home + ' ' + rScript_dir + 'send.R')#Writes the desired contents to the file
#infile.close()#Closes the file

#subprocess.call([reqDir + '\\testBat2.bat'])
#os.remove(reqDir + '\\testBat2.bat')    
"""
