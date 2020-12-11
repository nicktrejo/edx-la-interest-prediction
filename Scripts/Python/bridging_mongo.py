# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 18:07:57 2019

@author: albbl
Modified by nicktrejo
"""
#%% First Step

# CAN EXECUTE THIS FILE FORM A PYTHON CONSOLE USING
# exec(open("/home/nicolas/Documents/MasterII/TFM/Proyecto/bridging_mongo.py").read())

from datetime import datetime
# import json  # JSON encoder and decoder
import os  # Miscellaneous operating system interfaces
from pathlib import Path  # Object-oriented filesystem paths
import re  # Regular expression operations
import sys  # System-specific parameters and functions

import pandas as pd  # TO BE ABLE TO USE DATAFRAMES
import yaml  # PyYAML is a YAML parser and emitter for Python

from edxevent import EdxEvent
try:
    from storage.edxmongodbstore import EdxMongoStore
except ModuleNotFoundError:
    from edxmongodbstore import EdxMongoStore


# Only useful for debugging and coding purposes
def time_lapse_counter(timer) -> list:
    """
    Add one lapse to timer and print last lap and accumulated time

    :param timer: timer with different checkpoints
    :type timer: list
    :return: updated timer
    :rtype: list
    """

    lap_num = len(timer)
    lap_time = datetime.now()
    previous_lap_time = timer[-1]
    start_time = timer[0]
    _timer = timer.copy()
    _timer.append(lap_time)
    print('LAP {}\tElapsed time: {} (accumulated {})'.format(lap_num,
        lap_time - previous_lap_time, lap_time - start_time))
    return _timer


def set_course_info(db, course_name, course_id) -> None:
    """
    Insert or update course info with course_id and course_name

    :param db: object to connect to MongoDB
    :param course_name: Comprehensible name of the course
    :param course_id: Name of the course
    :type db: EdxMongoStore
    :type course_name: str
    :type course_id: str
    """
    db.insertCourseInfo(course_name, course_id)


def process_users(users_df, course_id, db) -> None:
    """
    Update (if necessary) users in MongoDB (db), including course_id

    :param users_df: user's profiles (not annonimized)
    :param course_id: Id of the course (eg: course-v1:UAMx+WebApp+1T2019a)
    :param db: object to connect to MongoDB
    :type users_df: pandas.core.frame.DataFrame
    :type course_id: str
    :type db: MDB.EdxMongoStore
    """
    # TODO: user's profiles should not be used in future
    df_aux = users_df.copy()
    df_aux.insert(1, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    users_dict = df_aux.to_dict(orient='records')
    db.updateUsers(course_id, users_dict)


def get_type_summary(text) -> str:
    """
    Get the classification for the given event type text

    :param text: Event type to classify
    :type text: str
    :return: event type classification
    :rtype: str
    """
    if text is None or text == "":
        return None
    if text in PROBLEM_EVENT_TYPES:
        return 'problem'
    if text in VIDEO_EVENT_TYPES:
        return 'video'
    if text in NAVIGATION_EVENT_TYPES:
        return 'navigation'
    if text in FORUM_EVENT_TYPES:
        return 'forum'
    if text in ENROLLMENT_EVENT_TYPES:
        return 'enrollment'
    if text in CERTIFICATE_EVENT_TYPES:
        return 'certificate'
    return None


def process_indicators(indicators_df, course_id, db) -> None:
    """
    Prepare indicators_df and save them in MongoDB

    :param indicators_df: df with calculated indicators
    :param course_id: Name of the course
    :param db: object to connect to MongoDB
    :type indicators_df: pandas.core.frame.DataFrame
    :type course_id: str
    :type db: MDB.EdxMongoStore
    """
    df_aux = indicators_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveFinalIndicators(df_dict)


def process_seen_problems(seen_problems_df, course_id, db) -> None:
    """
    Prepare seen_problems_df and save them in MongoDB

    :param seen_problems_df: df with seen problems
    :param course_id: Name of the course
    :param db: object to connect to MongoDB
    :type seen_problems_df: pandas.core.frame.DataFrame
    :type course_id: str
    :type db: MDB.EdxMongoStore
    """
    df_aux = seen_problems_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveSeenProblems(df_dict)


def process_seen_videos(seen_videos_df, course_id, db) -> None:
    """
    Prepare seen_videos_df and save them in MongoDB

    :param seen_videos_df: df with seen videos
    :param course_id: Name of the course
    :param db: object to connect to MongoDB
    :type seen_videos_df: pandas.core.frame.DataFrame
    :type course_id: str
    :type db: MDB.EdxMongoStore
    """
    df_aux = seen_videos_df.copy()
    df_aux.insert(0, 'course_id', course_id)
    df_aux['user_id'] = df_aux['user_id'].astype(int)
    df_dict = df_aux.to_dict(orient='records')
    db.saveSeenVideos(df_dict)


def get_indicators_aggr(db, course_id)  -> list:
    """
    Get final indicators aggregated for each user in course_id from db

    :param db: object to connect to MongoDB
    :param course_id: Name of the course
    :type db: MDB.EdxMongoStore
    :type course_id: str
    :return: Final Indicators aggregated for each user in course_id
    :rtype: list
    """
    data = db.getFinalIndicatorsAggrData(course_id)
    return data


def export_indicators_aggr(db, course_id, route) -> pd.DataFrame:
    """
    Get Final Indicators aggr from db, save them as csv in route and return df

    :param db: object to connect to MongoDB
    :param course_id: Name of the course
    :param route: Output folder
    :type db: MDB.EdxMongoStore
    :type course_id: str
    :type route: Path
    :return: Final Indicators aggregated for each user
    :rtype: pandas.core.frame.DataFrame
    """
    info = get_indicators_aggr(db, course_id)
    if not info:
        return
    info_df = pd.DataFrame(info)
    # Reorder columns to match expected output
    cols_order = ['username', 'user_id', 'num_events', 'num_sessions',
                  'video_time', 'problem_time', 'nav_time', 'forum_time',
                  'total_time', 'forum_events', 'nav_events',
                  'problem_events', 'video_events',  # enrollment, certificate ??
                  'consecutive_inactivity_days', 'connected_days',
                  'different_videos', 'different_problems']
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'aggrData.csv', encoding='utf-8', index=False)
    return info_df


def export_seen_videos(db, course_id, route) -> pd.DataFrame:
    """
    Get seen videos from db, save them as csv in route and return df

    :param db: object to connect to MongoDB
    :param course_id: Name of the course
    :param route: Output folder
    :type db: MDB.EdxMongoStore
    :type course_id: str
    :type route: Path
    :return: Seen Videos for each user in course_id
    :rtype: pandas.core.frame.DataFrame
    """
    info = db.getSeenVideos(course_id)
    if not info:
        return
    info_df = pd.DataFrame(info)
    info_df = info_df.drop(columns=['_id'])
    # Reorder to match expected output
    cols_order = ['user_id', 'dt_date', 'video_id', 'video_code']
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'videoInfo.csv', encoding='utf-8', index=False)
    return info_df


def export_seen_problems(db, course_id, route) -> pd.DataFrame:
    """
    Get seen problems from db, save them as csv in route and return df

    :param db: object to connect to MongoDB
    :param course_id: Name of the course
    :param route: Output folder
    :type db: MDB.EdxMongoStore
    :type course_id: str
    :type route: Path
    :return: Seen Problems for each user in course_id
    :rtype: pandas.core.frame.DataFrame
    """
    info = db.getSeenProblems(course_id)
    if not info:
        return
    info_df = pd.DataFrame(info)
    info_df = info_df.drop(columns=['_id'])
    # Reorder to match expected output
    cols_order = ['user_id', 'dt_date', 'grade', 'max_grade', 'attempts',
                  'module_id']
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'grades_problemInfo.csv',
                   encoding='utf-8', index=False)
    return info_df


def export_all_indicators(db, course_id, route) -> pd.DataFrame:
    """
    Get all indicators from db, save them as csv in route dir and return df

    :param db: object to connect to MongoDB
    :param course_id: Name of the course
    :param route: Output folder
    :type db: MDB.EdxMongoStore
    :type course_id: str
    :type route: Path
    :return: All indicators for each user in course_id
    :rtype: pandas.core.frame.DataFrame
    """
    info = db.getFinalIndicatorsData(course_id)
    info_df = pd.DataFrame(info)
    #Reorder to match expected output
    cols_order = ['user_id', 'dt_date', 'num_events', 'num_sessions',
                  'video_time', 'problem_time', 'nav_time', 'forum_time',
                  'total_time', 'video_events', 'problem_events',
                  'forum_events', 'nav_events',  # certificate ??
                  'consecutive_inactivity_days', 'connected_days',
                  'different_videos', 'different_problems', 'enrollment_mode']
    info_df = info_df[cols_order]
    info_df.to_csv(route / 'allIndicators.csv', encoding='utf-8', index=False)
    return info_df


def export_current_inactivity_days(db, course_id, route) -> pd.DataFrame:
    """
    Get Current Inactivity Days from db, save them as csv in route and return df

    :param db: object to connect to MongoDB
    :param course_id: Name of the course
    :param route: Output folder
    :type db: MDB.EdxMongoStore
    :type course_id: str
    :type route: Path
    :return: Current Inactivity Days for each user in course_id
    :rtype: pandas.core.frame.DataFrame
    """
    info = db.getCurrentInactivityDays(course_id)
    info_df = pd.DataFrame(info)
    info_df = info_df.astype({'consecutive_inactivity_days': 'int32'})
    info_df.to_csv(route / 'user_current_inactivity_days.csv',
                   encoding='utf-8', index=False)
    return info_df


if __name__ == '__main__':
    # TODO: Search for TREJO_PC and fix
    TREJO_PC = True  # Change to False

    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#video-interaction-events
    # https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html#video-interaction-events
    VIDEO_EVENT_TYPES = (
        'hide_transcript',
        'edx.video.transcript.hidden',
        'load_video',
        'edx.video.loaded',  # paired with 'load_video' (Mobile)
        'pause_video',
        'edx.video.paused',  # paired with 'pause_video' (Mobile)
        'play_video',
        'edx.video.played',  # paired with 'play_video' (Mobile)
        'seek_video',
        'edx.video.position.changed',  # paired with 'seek_video' (Mobile)
        'show_transcript',
        'edx.video.transcript.shown',  # paired with 'show_transcript' (Mobile)
        'speed_change_video',
        'stop_video',
        'edx.video.stopped',  # paired with 'stop_video' (Mobile)
        'video_hide_cc_menu',
        'video_show_cc_menu',
    )

    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#problem-interaction-events
    # https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html#problem-interaction-events
    PROBLEM_EVENT_TYPES = (
        'edx.problem.hint.demandhint_displayed',
        'edx.problem.hint.feedback_displayed',
        'problem_check',  # (Browser)
        # 'problem_check',  # (Server)
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
        'showanswer',
    )

    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#navigational-events
    # https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html#course-navigation-events
    NAVIGATION_EVENT_TYPES = (
        'page_close',
        'seq_goto',
        'seq_next',
        'seq_prev',
        'edx.ui.lms.link_clicked',
        'edx.ui.lms.outline.selected',
        'edx.ui.lms.sequence.next_selected',
        'edx.ui.lms.sequence.previous_selected',
        'edx.ui.lms.sequence.tab_selected',
    )

    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#discussion-forum-events
    # https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html#discussion-forum-events
    FORUM_EVENT_TYPES = (
        'edx.forum.comment.created',
        'edx.forum.response.created',
        'edx.forum.response.voted',
        'edx.forum.searched',
        'edx.forum.thread.created',
        'edx.forum.thread.voted',
        'edx.forum.thread.viewed',
    )

    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#enrollment-events
    # https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html#enrollment-events
    ENROLLMENT_EVENT_TYPES = (
        'edx.course.enrollment.activated',
        'edx.course.enrollment.deactivated',
        'edx.course.enrollment.mode_changed',
        # 'edx.course.enrollment.upgrade.clicked',  # events not used
        # 'edx.course.enrollment.upgrade.succeeded',  # events not used
    )

    # TODO: Add Certificate events to the processor
    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#certificate-events
    # https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html#certificate-events
    CERTIFICATE_EVENT_TYPES = (
        'edx.certificate.created',
        # 'edx.certificate.shared',
        # 'edx.certificate.evidence_visited',
    )

    # Obtained from:
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html#poll-and-survey-events
    POLL_AND_SURVEY_EVENT_TYPES = (
        'xblock.poll.submitted',
        'xblock.poll.view_results',
        'xblock.survey.submitted',
        'xblock.survey.view_results',
    )

    ACCOUNTABLE_TYPES = ('video', 'problem', 'navigation', 'forum')

    print('Python %s on %s' % (sys.version, sys.platform))
    # sys.path.extend(['/home/nicolas/Documents/MasterII/TFM/Proyecto'])
    # sys.path.extend(['/home/nicolas/ACCOMP'])

    time_lapse = []
    time_lapse.append(datetime.now())
    print('Starting at: {}'.format(time_lapse[-1]))

    with open('config.yaml', 'r') as config_file:
        CONFIG = yaml.load(config_file, Loader=yaml.SafeLoader)

    # Course info
    # TODO: In the future it could work with multiple courses
    courses = CONFIG['courses']
    course_id = courses[0]['course_id']  # -> it works for the first element
    course_name = courses[0]['course_name']  # -> it works for the first element
    # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/sql_schema.html#course-id
    # org_course_run = re.search(':(.*)', course_id).group(1)

    # Connection to MongoDB according to configuration
    CONFIG_CONN = CONFIG['mongo_connection']
    DB_HOST = CONFIG_CONN['host']
    DB_PORT = CONFIG_CONN['port']
    DB_NAME = CONFIG_CONN['dbname']
    DB_USER = None
    DB_PWD = None
    if 'user' in CONFIG_CONN:
        DB_USER = CONFIG_CONN['user']
    if 'pwd' in CONFIG_CONN:
        DB_PWD = CONFIG_CONN['pwd']
    if DB_USER and DB_PWD:
        db = EdxMongoStore(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PWD)
    else:
        db = EdxMongoStore(DB_HOST, DB_PORT, DB_NAME)

    if db.getCourseInfo(course_id) is False:
        set_course_info(db, course_name, course_id)
    course_info_mdb = db.getCourseInfo(course_id)

    # TODO: hardcoded value - should be corrected
    if not course_info_mdb['last_log_date']:
        course_info_mdb['last_log_date'] = datetime.min


    # Directories
    ROOT_DIR = Path(CONFIG['rootDir'])  # '/home/nicolas/ACCOMP'
    REQ_DIR = ROOT_DIR / 'REQUIRED_FILES'
    EVENTS_DIR = REQ_DIR / 'Events_ALL'

    # Files: logs and student profiles (we just need their user_id from the latter)
    last_date = datetime.min
    routes_aux = []
    for root, dirs, files in os.walk(EVENTS_DIR):
        for file in files:
            match = re.search(r'.*events-([0-9\-]*)-edx.txt', file)
            if match:
                file_date_str = match.group(1)
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                if file_date > course_info_mdb['last_log_date']:
                    routes_aux.append((root, file))
                    if last_date < file_date:
                        last_date = file_date
    routes = pd.DataFrame(routes_aux, columns=['path', 'filename'])
    del routes_aux  # delete out of memory

    if len(routes) == 0:
        print("No new routes, it'll crash")
    else:
        print('{} files to process'.format(len(routes)))

    OUTPUT_DIR = ROOT_DIR / ('OUTPUT' + '-' + last_date.strftime('%Y-%m-%d'))
    if OUTPUT_DIR.exists() is False:
        OUTPUT_DIR.mkdir()

    # TODO: user's profiles should not be used in future (it is not anonymized)
    STUDENT_PROFILE = REQ_DIR / CONFIG['filenames']['student_profile_file']
    general = pd.read_csv(STUDENT_PROFILE, encoding='utf-8')
    # TODO: FOLLOWING LINE COMMENTED BY nicktrejo
    general = general[general['enrollment_mode'] == 'verified']  # IMPORTANT: just verified students
    general.rename(columns={'id': 'user_id'}, inplace=True)
    process_users(general, course_id, db)

    read_data = []  # This will have a dictionary for every usefull event
    # Processing. This may take some time, perhaps 5 ~ 10 min
    for index, row in routes.iterrows():
        print('Read log file # {} / {}'.format(index+1, len(routes)))
        file_name = row['path'] + '/' + row['filename']

        with open(file_name, 'r') as f_events:
            for line in f_events:
                # avoid empty lines
                if line in [None, '\n', '\r\n', '\r', '']:
                    continue

                edx_event = EdxEvent(line)

                user_id = edx_event.get_user_id()
                course_id_read = edx_event.get_course_id()
                time = edx_event.get_time()

                # time_str = edx_event['time']
                # time = datetime.fromisoformat(time_str)  # if python 3.7+
                # time_date = time.date().isoformat()
                # time_time = time.time().isoformat()

                event_type = edx_event.get_event_type()
                type_summary = get_type_summary(event_type)

                if not user_id or not course_id or not time or not type_summary:
                    continue

                if not general['user_id'].eq(int(user_id)).any():
                    # user not in DataFrame general (profiles.csv and verified)
                    continue

                # Add new data as dictionary
                # and look for specific info depending on event_type
                read_data.append({'user_id': user_id,
                                  'event_type': event_type,
                                  'type_summary': type_summary,
                                  'dt_datetime': time,
                                  })

                if type_summary == 'video':
                    read_data[-1]['video_id'] = edx_event.get_video_id()
                    read_data[-1]['video_code'] = edx_event.get_video_code()

                elif type_summary == 'problem':
                    read_data[-1]['grade'] = edx_event.get_grade()
                    read_data[-1]['max_grade'] = edx_event.get_max_grade()
                    read_data[-1]['attempts'] = edx_event.get_attempts()
                    read_data[-1]['module_id'] = edx_event.get_module_id()

                elif type_summary == 'enrollment':
                    read_data[-1]['enrollment_mode'] = edx_event.get_enrollment_mode()

                elif type_summary == 'navigation':
                    # TODO: how to read these events ??
                    continue

                elif type_summary == 'forum':
                    # TODO: how to read these events ??
                    continue

                elif type_summary == 'certificate':
                    if edx_event.get_enrollment_mode() == 'verified':
                        read_data[-1]['enrollment_mode'] = 'certified'  # HARDCODED to certified
                    else:
                        read_data[-1]['enrollment_mode'] = edx_event.get_enrollment_mode()

    # INFO: creation of df {from log files}
    # columns = ['user_id', 'event_type', 'type_summary', 'dt_datetime', 'video_id',
    #            'video_code', 'grade', 'max_grade', 'attempts', 'module_id', 'enrollment_mode']

    df = pd.DataFrame(read_data)
    del read_data  # delete out of memory
    del index, row, file_name, line, edx_event,
    del user_id, time, event_type, type_summary, course_id_read,
    cols_ordered = ['user_id', 'event_type', 'type_summary', 'dt_datetime',
                    'video_id', 'video_code', 'grade', 'max_grade', 'attempts',
                    'module_id', 'enrollment_mode']
    df = df[cols_ordered]
    df['dt_datetime'] = pd.to_datetime(df['dt_datetime'])
    # INFO: end of creation of df
    # TODO: what to do with nan and None values ??

    # INFO: creation of unique_dates based on dt_datetime {from df} (ie: where there
    # is a useful event)
    unique_dates = df['dt_datetime'].dt.date.unique()
    unique_dates.sort()
    # unique_dates = unique_dates.sort()
    # INFO: end of creation of unique_dates
    # INFO: creation of daysToConsider using unique_dates (why?)
    # TODO: is daysToConsider necessary ??
    daysToConsider = pd.DataFrame(unique_dates, columns=['dt_date'])
    # INFO: end of creation of daysToConsider

    # INFO: creation of unique_users based on user_id {from df} (!= general)
    unique_users = df['user_id'].unique()
    unique_users.sort()
    # INFO: end of creation of unique_users

    # LAP 1	Elapsed time: 0:08:47.919238 (accumulated 0:08:47.919238)
    time_lapse = time_lapse_counter(time_lapse)

    #%% Second Step

    # INFO: creation of df_event_track selecting certain columns {from df} and
    # adding the columns dt_date and dt_time (all rows)
    df_event_track = df[['user_id', 'event_type', 'type_summary']].copy()
    df_event_track['dt_date'] = df['dt_datetime'].dt.date.copy()
    df_event_track['dt_time'] = df['dt_datetime'].dt.time.copy()
    df_event_track.to_csv(OUTPUT_DIR / 'allEvents.csv', encoding='utf-8', index=False)
    # INFO: end of creation of df_event_track

    # INFO: creation of df_event_track_accountable, filtering certain
    # type_summary (ACCUNTABLE_TYPES) {from df_event_track} (same columns)
    #Similar to allEvents.csv
    df_event_track_accountable = df_event_track.loc[df_event_track[[
        'type_summary']].isin(ACCOUNTABLE_TYPES)['type_summary']]
    df_event_track_accountable.to_csv(OUTPUT_DIR / 'allEvents_accountabilityFiltered.csv',
                                      encoding='utf-8', index=False)
    # INFO: end of creation of df_event_track_accountable

    # INFO: creation of df_distinct_events_per_user which counts by user, dt_date
    # and type_summary how many events each user has {from df_event_track}
    # Columns: [ ] TODO: pending
    # TODO: df_event_track or df_event_track_accountable ?
    df_distinct_events_per_user = df_event_track.groupby(['user_id', 'type_summary',
                                                          'dt_date']).size().reset_index(name='Freq')
    df_distinct_events_per_user.to_csv(OUTPUT_DIR / 'distinct_events_per_user.csv',
                                       encoding='utf-8', index=False)
    # INFO: end of creation of df_distinct_events_per_user (later on types are changed)

    # INFO: creation of multi_index as unique_users x unique_dates
    multi_index = pd.MultiIndex.from_product([unique_users, unique_dates],
                                             names=['user_id', 'dt_date'])
    # INFO: end of creation of multi_index

    # INFO: creation of df_events_per_user using (user_id x dt_date) which counts
    # by user, dt_date how many events they have (independent of type_summary)
    # {from df_event_track}
    df_events_per_user = df_event_track.groupby(['user_id', 'dt_date']).size()
    df_events_per_user = df_events_per_user.reindex(index=multi_index, fill_value=0)
    df_events_per_user = df_events_per_user.reset_index(name='num_events')
    df_events_per_user['dt_date'] = df_events_per_user['dt_date'].dt.date
    df_events_per_user.to_csv(OUTPUT_DIR / 'events_User.csv', encoding='utf-8', index=False)
    # INFO: end of creation of df_events_per_user

    # LAP 2	Elapsed time: 0:00:02.796921 (accumulated 0:08:50.716159)
    time_lapse = time_lapse_counter(time_lapse)


    # INFO: creation of df_sessions_per_user {from df}
    # The following lines of code count the number of sessions for every user for
    # every day (a new session occurs when there is no previous event in the last
    # 5 minutes)
    df_aux = df[['user_id', 'dt_datetime']].copy()
    df_aux['aux'] = 1
    df_aux['dt_date'] = df_aux['dt_datetime'].dt.date.apply(str)
    df_aux.sort_values('dt_datetime', axis=0, inplace=True)
    rolling_aux = df_aux.set_index('dt_datetime').groupby(['user_id', 'dt_date'])[
        'aux'].rolling('5min')
    df_aux = rolling_aux.apply(lambda x: len(x) < 2, raw=True).rename('num_sessions')
    df_aux = df_aux.reset_index(level=['dt_datetime']).groupby(['user_id', 'dt_date']).sum()
    df_aux.index = pd.MultiIndex.from_tuples([(x[0], pd.to_datetime(x[1])) for x in df_aux.index],
                                             names=df_aux.index.names)
    df_aux = df_aux.reindex(index=multi_index, fill_value=0)
    # df_aux.reset_index(level=['user_id','dt_date'], inplace=True)

    df_sessions_per_user = df_aux.reset_index(level=['user_id', 'dt_date'])
    df_sessions_per_user.to_csv(OUTPUT_DIR / 'sessions_User.csv', encoding='utf-8', index=False)
    # INFO: end of creation of df_sessions_per_user

    # LAP 3	Elapsed time: 0:00:03.024266 (accumulated 0:08:53.740425)
    time_lapse = time_lapse_counter(time_lapse)

    # INFO: creation of df_time_per_event_type {from df}
    # Similar to previous code, but to calculate time spent per event_type
    session_time_or_nothing = lambda x: x[1] - x[0] if x[1] - x[0] < 300 else 0
    # I think it should be with 300 (5min) rather than 0:
    # session_time_or_nothing = lambda x: x[1] - x[0] if x[1] - x[0] < 300 else 300
    # session_time_or_nothing = lambda x: min(x[1] - x[0], 300)
    df_aux = df[['user_id', 'dt_datetime', 'type_summary']].copy()
    df_aux['dt_seconds'] = df['dt_datetime'].apply(lambda x: x.timestamp())
    df_aux['dt_date'] = df_aux['dt_datetime'].dt.date.apply(str)
    df_aux.sort_values('dt_datetime', axis=0, ascending=True, inplace=True)
    rolling_aux = df_aux.set_index('dt_datetime').groupby(['user_id', 'dt_date',
        'type_summary'])['dt_seconds'].rolling(window=2)
    df_aux = rolling_aux.apply(session_time_or_nothing, raw=True).rename('session_time')
    df_aux = df_aux.reset_index(level=['dt_datetime']).groupby([
        'user_id', 'dt_date', 'type_summary']).sum()
    df_aux = df_aux.unstack(level='type_summary', fill_value=0)
    df_aux.columns = df_aux.columns.droplevel(level=0)
    df_aux.index = pd.MultiIndex.from_tuples([(x[0], pd.to_datetime(x[1])) for x in df_aux.index],
                                             names=df_aux.index.names)
    df_aux = df_aux.reindex(index=multi_index, fill_value=0)
    df_time_per_event_type = df_aux.reset_index(level=['user_id', 'dt_date'])
    df_time_per_event_type['total_time'] = df_time_per_event_type.iloc[:, 1:].sum(axis=1)
    df_time_per_event_type.to_csv(OUTPUT_DIR / 'time_per_event_User.csv', encoding='utf-8', index=False)
    # INFO: end of creation of df_time_per_event_type

    # LAP 4	Elapsed time: 0:00:05.707956 (accumulated 0:08:59.448381)
    time_lapse = time_lapse_counter(time_lapse)

    ####################################################

    # INFO: ERROR: modification of df_events_per_user out of place
    df_events_per_user['dt_date'] = pd.to_datetime(df_events_per_user['dt_date'],
                                                   format='%Y-%m-%d')
    df_events_per_user['user_id'] = df_events_per_user['user_id'].apply(str)

    # INFO: ERROR: modification of df_sessions_per_user out of place
    df_sessions_per_user['dt_date'] = pd.to_datetime(
        df_sessions_per_user['dt_date'], format='%Y-%m-%d')
    df_sessions_per_user['user_id'] = df_sessions_per_user['user_id'].apply(str)

    # INFO: ERROR: modification of df_time_per_event_type out of place
    df_time_per_event_type['dt_date'] = pd.to_datetime(
        df_time_per_event_type['dt_date'], format='%Y-%m-%d')
    df_time_per_event_type['user_id'] = df_time_per_event_type['user_id'].apply(str)

    # INFO: ERROR: modification of df_distinct_events_per_user out of place
    df_distinct_events_per_user['dt_date'] = pd.to_datetime(
        df_distinct_events_per_user['dt_date'], format='%Y-%m-%d')
    df_distinct_events_per_user['user_id'] = df_distinct_events_per_user['user_id'].apply(str)


    # INFO: creation of finalDf . Very complex (next 150 lines)
    # Cross join between studentsToConsider and daysToConsider
    # COULD THIS BE USED??
    # multi_index = pd.MultiIndex.from_product([unique_users, unique_dates], names=['user_id', 'time_day'])

    studentsToConsider = pd.DataFrame(general['user_id'].unique(), columns=['user_id'])
    studentsToConsider['key'] = 0
    daysToConsider['key'] = 0
    finalDf = studentsToConsider.merge(daysToConsider, on='key')
    finalDf['user_id'] = finalDf['user_id'].apply(str)
    finalDf['dt_date'] = pd.to_datetime(finalDf['dt_date'], format='%Y-%m-%d')
    finalDf.drop(labels=['key'], axis='columns', inplace=True)
    studentsToConsider.drop(labels=['key'], axis='columns', inplace=True)
    daysToConsider.drop(labels=['key'], axis='columns', inplace=True)
    # SHORTER WAY OF DOING THE SAME
    # unique_users_2 = general['user_id'].unique()
    # unique_users_2.sort()
    # finalDf = pd.MultiIndex.from_product([unique_users_2,
    #                                       daysToConsider['dt_date']]
    #                                      ).to_frame(index=False, name=['dt_date', 'user_id'])
    # finalDf['user_id'] = finalDf['user_id'].apply(str)
    # finalDf['dt_date'] = pd.to_datetime(finalDf['dt_date'], format='%Y-%m-%d')


    # LAP 5	Elapsed time: 0:00:00.324110 (accumulated 0:08:59.772491)
    time_lapse = time_lapse_counter(time_lapse)


    finalDf = pd.merge(finalDf, df_events_per_user, how='left', on=['user_id', 'dt_date'])
    finalDf = pd.merge(finalDf, df_sessions_per_user, how='left', on=['user_id', 'dt_date'])
    finalDf = pd.merge(finalDf, df_time_per_event_type, how='left', on=['user_id', 'dt_date'])

    columns_mapped = {
        'video': 'video_time',
        'problem': 'problem_time',
        'navigation': 'nav_time',
        'forum': 'forum_time',
        'enrollment': 'enrollment_time',
        # 'certificate' ??
    }
    finalDf.rename(columns=columns_mapped, inplace=True)


    df_aux = df_distinct_events_per_user.set_index(['user_id', 'dt_date', 'type_summary'])
    df_aux = df_aux.unstack(level=2, fill_value = 0)
    df_aux.columns = df_aux.columns.droplevel(level=0)
    # df_aux = df_aux.reindex(index=multi_index, fill_value=0)
    df_aux = df_aux.reset_index(level=['user_id', 'dt_date'])
    columns_mapped = {
        'navigation': 'nav_events',
        'problem': 'problem_events',
        'video': 'video_events',
        'forum': 'forum_events',
        'enrollment': 'enrollment_events',
        # 'certificate' ??
    }
    df_aux.rename(columns=columns_mapped, inplace=True)

    finalDf = pd.merge(finalDf, df_aux, how='left', on=['user_id', 'dt_date'])
    finalDf = finalDf.fillna(0)


    # LAP 6	Elapsed time: 0:00:00.448205 (accumulated 0:09:00.220696)
    time_lapse = time_lapse_counter(time_lapse)


    ########## GET CONSECUTIVE INACTIVITY DAYS + DAYS CONNECTED

    # finalDf must be sorted by user_id, dt_date
    finalDf['activity_aux'] = finalDf[finalDf.columns[2:]].any('columns')  # TO CHECK!
    finalDf['consecutive_inactivity_days'] = 0
    finalDf['connected_days'] = 0

    for student in finalDf['user_id'].unique():
        # TODO: improve (INEFICIENT CALLS TO DB)
        inact_ctr, conn_ctr = db.getUserLastActivityInfo(int(student), course_id)
        student_indexes = finalDf[finalDf['user_id'] == student].index
        for i in student_indexes:
            if finalDf.at[i, 'activity_aux'] == 0:
                inact_ctr += 1
            else:
                inact_ctr =  0
                conn_ctr += 1
            finalDf.at[i, 'consecutive_inactivity_days'] = inact_ctr
            finalDf.at[i, 'connected_days'] = conn_ctr
    del student_indexes
    finalDf.drop(['activity_aux'], axis='columns', inplace=True)

    # LAP 7	Elapsed time: 0:02:25.685283
    time_lapse = time_lapse_counter(time_lapse)

    # TODO : improve
    seenVideos = df[['user_id', 'dt_datetime', 'video_id',
                     'video_code']][df['type_summary'] == 'video'].copy()
    seenVideos['dt_datetime'] = pd.to_datetime(seenVideos['dt_datetime'].dt.date, format='%Y-%m-%d')
    # seenVideos['dt_datetime'] = pd.to_datetime(seenVideos['dt_datetime'].dt.date.apply(str), format='%Y-%m-%d')
    seenVideos.rename(columns={'dt_datetime': 'dt_date'}, inplace=True)
    seenVideos.sort_values('dt_date', axis=0, ascending=True, inplace=True)

    seenVideos_2 = seenVideos[['user_id', 'dt_date', 'video_id']].copy()
    seenVideos_2 = seenVideos_2.groupby(['user_id','video_id']).first()
    seenVideos_2 = seenVideos_2.reset_index()
    seenVideos_2 = seenVideos_2.groupby(['user_id','dt_date']).count()
    seenVideos_2.rename(columns={'video_id': 'different_videos'}, inplace=True)
    seenVideos_2 = seenVideos_2.reindex(index=multi_index, fill_value=0)

    seenProblems = df[['user_id', 'dt_datetime', 'grade', 'max_grade', 'attempts',
                       'module_id']][df['type_summary'] == 'problem'].copy()
    seenProblems['dt_datetime'] = pd.to_datetime(seenProblems['dt_datetime'].dt.date, format='%Y-%m-%d')
    # seenProblems['dt_datetime'] = pd.to_datetime(seenProblems['dt_datetime'].dt.date.apply(str), format='%Y-%m-%d')
    seenProblems.rename(columns={'dt_datetime': 'dt_date'}, inplace=True)
    seenProblems.sort_values('dt_date', axis=0, ascending=True, inplace=True)

    seenProblems_2 = seenProblems[['user_id', 'dt_date', 'module_id']].copy()
    seenProblems_2 = seenProblems_2.groupby(['user_id','module_id']).first()
    seenProblems_2 = seenProblems_2.reset_index()
    seenProblems_2 = seenProblems_2.groupby(['user_id','dt_date']).count()
    seenProblems_2.rename(columns={'module_id': 'different_problems'}, inplace=True)
    seenProblems_2 = seenProblems_2.reindex(index=multi_index, fill_value=0)

    # Insert in the first date the previous accumulated videos/problems from db
    # for each user_id
    for student in seenVideos_2.index.get_level_values('user_id').unique():
        elemCtrVid, elemCtrPro = db.getUserLastSeenInfo(student, course_id)
        seenVideos_2.loc[(student, unique_dates.min()), "different_videos"] += elemCtrVid
        seenProblems_2.loc[(student, unique_dates.min()), "different_problems"] += elemCtrPro


    # Calculate accumulated sum for each user
    seenVideos_2 = seenVideos_2.groupby(level='user_id').cumsum()
    seenVideos_2 = seenVideos_2.reset_index()
    seenVideos_2['user_id'] = seenVideos_2['user_id'].apply(str)
    seenProblems_2 = seenProblems_2.groupby(level='user_id').cumsum()
    seenProblems_2 = seenProblems_2.reset_index()
    seenProblems_2['user_id'] = seenProblems_2['user_id'].apply(str)

    finalDf = pd.merge(finalDf, seenVideos_2, how='left', on=['user_id', 'dt_date'])
    finalDf = pd.merge(finalDf, seenProblems_2, how='left', on=['user_id', 'dt_date'])


    # TODO: IMPROVE THIS CODE !
    ########## SET NUM DIFF VIDEOS + PROBLEMS


    # LAP 8	Elapsed time: 0:00:01.829443
    time_lapse = time_lapse_counter(time_lapse)

    # ENROLLMENT MODE THROUGH TIME:  audit >> verified >> certified

    mask_mode_changed = df.event_type == 'edx.course.enrollment.mode_changed'
    mask_activated = df.event_type == 'edx.course.enrollment.activated'
    mask_certified = df.event_type == 'edx.certificate.created'
    df_enr_mode_changed = df[mask_mode_changed][['user_id', 'enrollment_mode']].copy()
    df_enr_activated = df[mask_activated][['user_id', 'enrollment_mode']].copy()
    df_enr_certified = df[mask_certified][['user_id', 'enrollment_mode']].copy()

    df_enr_mode_changed['dt_date'] = df[mask_mode_changed]['dt_datetime'].dt.date
    df_enr_activated['dt_date'] = df[mask_activated]['dt_datetime'].dt.date
    df_enr_certified['dt_date'] = df[mask_certified]['dt_datetime'].dt.date

    df_enr_mode_changed.sort_values(by=['user_id', 'dt_date'], inplace=True)
    df_enr_activated.sort_values(by=['user_id', 'dt_date'], inplace=True)
    df_enr_certified.sort_values(by=['user_id', 'dt_date'], inplace=True)

    # Need to be previously sorted (at least by dt_date)
    df_enr_mode_changed = df_enr_mode_changed.groupby('user_id').first()
    df_enr_activated = df_enr_activated.groupby('user_id').first()
    df_enr_certified = df_enr_certified.groupby('user_id').first()

    # Join the three DataFrames in one
    # df_enr = df_enr_mode_changed.append(other=df_enr_activated)
    df_enr = pd.concat([df_enr_mode_changed, df_enr_activated, df_enr_certified])
    del df_enr_mode_changed, df_enr_activated, df_enr_certified

    df_enr = df_enr.reset_index()
    df_enr = df_enr.groupby(['user_id', 'dt_date']).first()
    df_enr = df_enr.reindex(index=multi_index).reset_index()
    df_enr['enrollment_mode'] = df_enr.groupby('user_id')['enrollment_mode'].fillna(method='ffill')
    df_enr['user_id'] = df_enr['user_id'].apply(str)

    """
    df_enr_mode_changed = df_enr_mode_changed.set_index(keys='dt_date', append=True)
    df_enr_activated = df_enr_activated.set_index(keys='dt_date', append=True)
    
    df_enr_mode_changed = df_enr_mode_changed.reindex(index=multi_index).reset_index()
    df_enr_activated = df_enr_activated.reindex(index=multi_index).reset_index()
    
    df_enr_mode_changed['enrollment_mode'] = df_enr_mode_changed.groupby('user_id')['enrollment_mode'].fillna(method='ffill')
    df_enr_activated['enrollment_mode'] = df_enr_activated.groupby('user_id')['enrollment_mode'].fillna(method='ffill')
    """

    finalDf = pd.merge(finalDf, df_enr, how='left', on=['user_id', 'dt_date'])

    # LAP 9	Elapsed time: 0:00:00.504337
    time_lapse = time_lapse_counter(time_lapse)


    finalDf.to_csv(OUTPUT_DIR / 'allIndicators.csv', encoding='utf-8', index=False)
    process_indicators(finalDf, course_id, db)

    # LAP 10	Elapsed time: 0:00:10.447328
    time_lapse = time_lapse_counter(time_lapse)


    ################ AGGREGATED DATA
    final_aggrData = finalDf.groupby(['user_id']).agg({
        'num_events': ['sum'],
        'num_sessions': ['sum'],
        'enrollment_time': ['sum'],
        'forum_time': ['sum'],
        'nav_time': ['sum'],
        'problem_time': ['sum'],
        'video_time': ['sum'],
        'total_time': ['sum'],
        'consecutive_inactivity_days': ['max'],
        'connected_days': ['max'],
        'different_videos': ['max'],
        'different_problems': ['max'],
        'enrollment_mode': ['last'],
    })
    final_aggrData.columns = final_aggrData.columns.droplevel(level=1)


    ######################################################
    # Get usernames for final 'user_id's
    general['user_id'] = general.user_id.astype(str)

    final_aggrData = pd.merge(final_aggrData, general[['user_id', 'username']],
                              how='left', on='user_id')

    # Re-arrange columns so that 'username' is the first one
    cols = final_aggrData.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    final_aggrData = final_aggrData[cols]


    # LAP 11	Elapsed time: 0:00:00.091409
    time_lapse = time_lapse_counter(time_lapse)

    # Relevant info To Mongo
    process_seen_videos(seenVideos, course_id, db)
    process_seen_problems(seenProblems, course_id, db)

    finalAggrData = export_indicators_aggr(db, course_id, OUTPUT_DIR)
    finalSeenVideos = export_seen_videos(db, course_id, OUTPUT_DIR)
    finalSeenProblems = export_seen_problems(db, course_id, OUTPUT_DIR)
    finalAllIndicators = export_all_indicators(db, course_id, OUTPUT_DIR)
    # TODO: (nicktrejo) I don't think it's a good idea that REQ_DIR is the output directory
    currentInactivityDays = export_current_inactivity_days(db, course_id, REQ_DIR)
    # final_aggrData.to_csv(resultsDir / 'aggrData.csv', encoding='utf-8', index=False)
    # seenProblems.to_csv(resultsDir / 'grades_problemInfo.csv', encoding='utf-8', index=False)
    # seenVideos.to_csv(resultsDir / 'videoInfo.csv', encoding='utf-8', index=False)

    # enrollmentData.to_csv(OUTPUT_DIR / 'enrollment.csv', encoding='utf-8', index=False)

    if not TREJO_PC:
        os.system('python parse_courseStructure.py') # Should have an argument referred to the course_str's file-path

    # LAP 12	Elapsed time: 0:00:48.990245
    time_lapse = time_lapse_counter(time_lapse)


    ####################

    #### ADDED BY nicktrejo ####
    # if not TREJO_PC:
    #     sys.exit()
    ############################


    COURSE_DIR = ROOT_DIR / 'Scripts' / 'Python'
    sys.path.append(COURSE_DIR)

    if not TREJO_PC:
        parseItem = courseParse.courseStrExtractor(REQ_DIR)

        courseStructure = parseItem.strParser(OUTPUT_DIR)
        process_course_structure(courseStructure, course_id, db)

    ################################
    # IMGS CONCAT TEST NEW VERSION #
    # Creates struct in output dir #
    ################################

    EMAIL_RESOURCES_TEMPLATE_DIR = REQ_DIR / 'ACCOMP_VISUAL'
    EMAIL_RESOURCES_RESULTS_DIR = OUTPUT_DIR / 'ACCOMP_VISUAL'

    if not TREJO_PC:
        concatItem = concat.imgConcatenator(REQ_DIR, EMAIL_RESOURCES_RESULTS_DIR,
                                            EMAIL_RESOURCES_TEMPLATE_DIR)


    #%% Third Step

    ###########################
    # EJECUTA LOS SCRIPTS DE R
    ###########################

    # r_home = '"'+os.environ['R_HOME']+'\\bin\\Rscript.exe"'
    # # To be able to write a .bat file with Rscript based on this route

    # import subprocess

    # rScript_dir = REQ_DIR.replace('REQUIRED_FILES', 'Scripts\\R\\')

    # infile = open(REQ_DIR + '\\testBat.bat', 'w') #Opens the file
    # infile.write(r_home + ' ' + rScript_dir + 'problemGrade_Adequation.R') #Writes the desired contents to the file
    # infile.close()  # Closes the file

    # subprocess.call([REQ_DIR + '\\testBat.bat'])
    # os.remove(REQ_DIR + '\\testBat.bat')

    ##########################################
    # ONCE IMAGES ARE CREATED, CREATE EMAILS #
    ##########################################

    # With False, it generates the extraTimeS
    if not TREJO_PC:
        concatItem.createEmails()


    ####################
    # EJECUTA SEND.R
    ####################


    """
tero = [x for x in dir() if x[0]!='_']
for x in set([str(type(globals()[x])).split("'")[1] for x in tero]):
    print('\n--->  {}\n'.format(x))
    for y in tero:
        if x == str(type(globals()[y])).split("'")[1]:
            print(y)

df.iloc[0].apply(type)
    """
