# -*- coding: utf-8 -*-

"""
Project: TFG edX-MAS
Author: Victor Macias Palla
Modified: Juan Soberon Higuera
Modified: Nicolas Trejo (2020)
Date last modified: May 2020

Module for insert data in Mongo store
"""

import csv
from datetime import datetime
from pathlib import Path  # changed by nicktrejo
from urllib.parse import quote_plus  # changed by nicktrejo

from pymongo import MongoClient

class EdxMongoStore():
    """
    The EdxMongoStore object allows to connect to MongoDB and perform CRUD operations

    :param host: ip address or 'localhost'
    :type host: str
    :param port: TCP port number (mongodb's default is 27017)
    :type port: int
    :param dbname: name for database to connect
    :type dbname: str
    :param username: username to access mongodb
    :type username: str
    :param pwd: password to access mongodb
    :type pwd: str
    """

    def __init__(self, host, port, dbname, username=None, pwd=None):
        """
        Constructor method
        """
        self.connect(host, port, dbname, username, pwd)
        self.course_runs_data = {'coll':'course_runs',
                                 'attributes':['course', 'run']}
        self.course_users_data = {'coll':'course_users',
                                  'attributes':['course_run_id', 'username']}
        self.indicators_data = {'coll':'indicators',
                                'attributes':['courserun_user_id', 'indicator_name', 'value',
                                              'day']}
        self.certificates_data = {'coll':'certificates',
                                  'attributes':['courserun_user_id', 'grade', 'status']}
        self.course_indicator_names_data = {'coll':'course_indicator_names',
                                            'attributes':['courserun_user_id', 'indicator_name']}
        self.course_duration_data = {'coll':'course_duration',
                                     'attributes':['course_run_id', 'duration']}
        self.course_prediction_meths_names_data = {'coll':'course_prediction_meths_names',
                                                   'attributes':['course_run_id', 'meth']}
        self.course_outputs_data = {'coll':'course_outputs',
                                    'attributes':['course_run_id', 'output']}
        self.dropout_data = {'coll':'dropout',
                             'attributes':['courserun_user_id', 'type', 'last_connection',
                                           'number_activity_days', 'status']}
        self.course_structure = {'coll':'course_structure',
                                 'attributes':['course_id', 'seq_block_txt', 'problem_id',
                                               'problem_wieght', 'problem_max_attemps']}
        self.final_indicators = {'coll':'final_indicators',
                                 'attributes':['course_id', 'user_id', 'dt_date', 'num_events',
                                               'num_sessions', 'video_time', 'problem_time',  # 'enrollment_time',
                                               'nav_time', 'forum_time', 'total_time',
                                               'forum_events', 'nav_events', 'video_events',  # 'enrollment_events',
                                               'problem_events', 'consecutive_inactivity_days',
                                               'connected_days', 'different_videos',
                                               'different_problems']}
        self.course_info = {'coll':'course_info',
                            'attributes':['course_id', 'course_name']}
        self.course_users = {'coll':'course_users',  # TODO: UPDATE
                             'attributes':['user_id', 'course_id', 'username',
                                           'name', 'email', 'language',
                                           'location', 'year_of_birth',
                                           'gender', 'level_of_education',
                                           'mailing_address', 'goals',
                                           'enrollment_mode',
                                           'verification_status', 'cohort',
                                           'city', 'country']}
        self.seen_videos = {'coll':'seen_videos',
                            'attributes':['course_id', 'user_id', 'dt_date',
                                          'video_id', 'video_code']}
        self.seen_problems = {'coll':'seen_problems',
                              'attributes':['course_id', 'user_id', 'dt_date', 'grade',
                                            'max_grade', 'attempts', 'module_id']}
        self.event_track = {'coll':'event_track',
                            'attributes':['course_id', 'user_id', 'event_type', 'type_summary',
                                          'time_day', 'time_hourDetail']}

    def connect(self, host, port, dbname, username=None, pwd=None) -> None:
        """
        Open a client connection to mongoDB and get the `dbname` database

        :param host: ip address or 'localhost'
        :type host: str
        :param port: TCP port number (mongodb's default is 27017)
        :type port: int
        :param dbname: name for database to connect
        :type dbname: str
        :param username: username to login to mongodb
        :type username: str
        :param pwd: password to login to mongodb
        :type pwd: str
        """
        db_host = host
        db_port = port
        db_name = dbname
        if db_host in ('localhost', '127.0.0.1'):
            self.client = MongoClient(db_host, db_port)
        else:
            db_username = quote_plus(username)  # changed by nicktrejo
            db_password = quote_plus(pwd)  # changed by nicktrejo
            self.client = MongoClient('mongodb://%s:%s@%s:%s' %
                                      (db_username, db_password, db_host, db_port))
        self.db = self.client[db_name]

    def buildMongoOne(self, attributes, data) -> dict:  # changed by nicktrejo
        """
        Build `data` as a dictionary with `attributes` as keys

        :param attributes: list of fields for the document
        :type attributes: list, tuple
        :param data: single document data
        :type data: list or iterable
        :return: built dictionary
        :rtype: dict
        """
        obj = dict(zip(attributes, data))
        return obj

    def buildMongoMany(self, attributes, data) -> list:  # changed by nicktrejo
        """
        Build `data` as list of dictionaries with `attributes` as keys

        :param attributes: list of fields for the document
        :type attributes: list, tuple
        :param data: multiple document data
        :type data: list or iterable
        :return: built list of dictionaries
        :rtype: list
        """
        array = [self.buildMongoOne(attributes, data_obj) for data_obj in data]  # changed nicktrejo
        return array

    def insertStatement(self, info, data) -> None:  # changed by nicktrejo
        """
        Insert one document with `data` in collection `info['coll']` using `info['attributes']` as fields

        :param info: dictionary with collection name and document fields
        :type info: dict
        :param data: single document data
        :type data: list or iterable
        """
        coll = self.db[info['coll']]
        new_data = self.buildMongoOne(info['attributes'], data)  # changed by nicktrejo
        coll.insert_one(new_data)

    def insertMultiple(self, info, data) -> None:  # changed by nicktrejo
        """
        Insert multiple documents with data in collection `info['coll']` using `info['attributes']` as fields

        :param info: dictionary with collection name and document fields
        :type info: dict
        :param data: multiple document data
        :type data: list or iterable
        """
        coll = self.db[info['coll']]
        new_data = self.buildMongoMany(info['attributes'], data)  # changed by nicktrejo
        coll.insert_many(new_data)

    def selectStatement(self, info, data) -> dict:  # changed by nicktrejo
        """
        Query a single document matching `data` in collection `info['coll']`

        :param info: dictionary with collection name
        :type info: dict
        :param data: query
        :type data: dict
        :return: single document
        :rtype: dict
        """
        coll = self.db[info['coll']]
        result = coll.find_one(data)
        return result

    ########################################
    # Course - Run
    def saveCourse(self, course, run) -> None:
        """
        Save `course` and `run` in corresponding collection

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        data = (course, run)
        self.insertStatement(self.course_runs_data, data)  # changed by nicktrejo

    def getCourseRunId(self, course, run) -> object:  # changed by nicktrejo
        """
        Get internal `_id` for given `course` and `run`

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :return: _id for given course and run
        :rtype: bson.objectid.ObjectId
        """
        query = {'course': course, 'run': run}
        result = self.selectStatement(self.course_runs_data, query)  # changed by nicktrejo
        return result.get('_id')

    ########################################
    # Course - Run - Users
    def saveUserInCourse(self, username, course, run) -> None:
        """
        Save user `username` in corresponding collection

        :param username: username in edx
        :type username: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        data = (course_run_id, username)
        self.insertStatement(self.course_users_data, data)  # changed by nicktrejo

    def getCourserunUserId(self, username, course, run) -> str:  # changed by nicktrejo
        """
        Get internal `_id` for given `username`, `course` and `run`

        :param username: username in edx
        :type username: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :return: _id for given username, course and run
        :rtype: ObjectId, str  # TODO: to check
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        query = {'course_run_id': course_run_id, 'username': username}
        result = self.selectStatement(self.course_users_data, query)  # changed by nicktrejo
        return result.get('_id')  # changed by nicktrejo


    def getUsersInCourse(self, course, run) -> list:
        """
        Get users for given `course` and `run`

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :return: list of usernames
        :rtype: list
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        query = {'course_run_id': course_run_id}
        course_run_users = self.db.course_users.find(query)
        # # ALTERNATIVE to previous line
        # coll = self.db[self.course_users_data['coll']]
        # course_run_users = coll.find(query)
        # # OTHER ALTERNATIVE (using project)
        # coll = self.db[self.course_users_data['coll']]
        # pipeline = [
        #     {
        #         '$match': {'course_run_id': course_run_id}
        #     },
        #     {
        #         '$project': {'username' : 1}
        #     },
        # ]
        # data_cursor = coll.aggregate(pipeline)
        # return [data for data in data_cursor]
        users = [user['username'] for user in course_run_users]
        return users

    def saveUsersFromCsv(self, f_users_data_name, course, run) -> None:
        """
        Read `user_id` for all users in `f_users_data_name` and insert them in corresponding collection

        :param f_users_data_name: file with users information
        :type f_users_data_name: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        with open(f_users_data_name, 'r') as f_users:  # changed by nicktrejo
            reader = csv.DictReader(f_users, delimiter=';')  # changed by nicktrejo
            # changed by nicktrejo
            reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]
            users = [row['user_id'] for row in reader]  # changed by nicktrejo

        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        args = [(course_run_id, user) for user in users]
        self.insertMultiple(self.course_users_data, args)  # changed by nicktrejo

    ########################################
    # Indicators
    def saveUserIndicator(self, course, run, indicator, username, value, day) -> None:
        """
        Save single indicator value for certain user and date in corresponding collection

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param indicator: name of the indicator
        :type indicator: str
        :param username: username in edx
        :type username: str
        :param value: value of the indicator
        :type value: str,int
        :param day: date
        :type day: date
        """
        courserun_user_id = self.getCourserunUserId(username, course, run)  # changed by nicktrejo
        data = (courserun_user_id, indicator, value, day)
        self.insertStatement(self.indicators_data, data)  # changed by nicktrejo

    def saveIndicatorsFromDict(self, users, dict_events, indicator_event_list, indicator_activity_list) -> None:
        """
        Save multiple indicators values for users in a date in corresponding collection
        Deprecated: Currently not used (apparently)

        :param users: list of usernames
        :type users: list
        :param dict_events: dictionary with usernames as keys and user_events as values
        :type dict_events: dict
        :param indicator_event_list: list (of dict?) with indicators info
        :type indicator_event_list: list
        :param indicator_activity_list: list (of dict?) with indicators info
        :type indicator_activity_list: list
        """
        if indicator_event_list:
            course = indicator_event_list[0].course
            run = indicator_event_list[0].run
            day = indicator_event_list[0].day
        elif indicator_activity_list:
            course = indicator_activity_list[0].course
            run = indicator_activity_list[0].run
            day = indicator_activity_list[0].day
        else:
            return

        args = []

        for user in dict_events:
            user_events = dict_events[user]
            courserun_user_id = self.getCourserunUserId(user, course, run)  # changed by nicktrejo
            for ind in indicator_event_list:
                value = ind.getIndicator(user_events)
                args.append((courserun_user_id, ind.indicator, value, day))

        for user in users:
            courserun_user_id = self.getCourserunUserId(user, course, run)  # changed by nicktrejo
            for ind in indicator_activity_list:
                value = ind.getIndicator(user, day)
                args.append((courserun_user_id, ind.indicator, value, day))

        self.insertMultiple(self.indicators_data, args)  # changed by nicktrejo

    ########################################
    # Certificates
    def saveUserCertificate(self, username, grade, status, course, run) -> None:
        """
        Save single value for certificate and user in
        corresponding collection (certificates_data)

        :param username: username in edx
        :type username: str
        :param grade: student's grade?
        :type grade: float  # isn't it?
        :param status: whether it is downloadable or not
        :type status: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        if status == 'downloadable':
            status_ = 1
        else:
            status_ = 0

        courserun_user_id = self.getCourserunUserId(username, course, run)  # changed by nicktrejo
        data = (courserun_user_id, grade, status_)

        self.insertStatement(self.certificates_data, data)  # changed by nicktrejo

    def saveCertificatesFromCsv(self, f_cert_data_name, course, run) -> None:
        """
        Save multiple values for certificates and users taken from csv in
        corresponding collection (certificates_data)

        :param f_cert_data_name: csv file with users and their grades (cert_file?)
        :type f_cert_data_name: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        with open(f_cert_data_name, 'r') as f_cert:  # changed by nicktrejo
            reader = csv.DictReader(f_cert, delimiter=';')  # changed by nicktrejo
            reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]  # changed by nicktrejo

            certs = [[row['user_id'], row['grade'], row['status']] for row in reader]  # changed by nicktrejo
            # not sure if 'username' instead of 'user_id'

        args = [(self.getCourserunUserId(cert[0], course, run), cert[1],
                 0 if cert[2] in ['notpassing', 'audit_notpassing'] else 1)
                for cert in certs]  # changed by nicktrejo
        self.insertMultiple(self.certificates_data, args)  # changed by nicktrejo

    ########################################
    # Dropout
    def saveUserDropout(self, username, mark_type, last_connection, number_activity_days, dropout, course, run) -> None:
        """
        Save single value for user dropout in corresponding collection (dropout_data)

        :param username: username in edx
        :type username: str
        :param mark_type: type ??
        :type mark_type: str  # isn't it?
        :param last_connection: last_connection??
        :type last_connection: int  # isn't it?
        :param number_activity_days: number_activity_days??
        :type number_activity_days: int  # isn't it?
        :param dropout: dropout??
        :type dropout: bool  # isn't it?
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        courserun_user_id = self.getCourserunUserId(username, course, run)  # changed by nicktrejo
        data = (courserun_user_id, mark_type, last_connection, number_activity_days, dropout)

        self.insertStatement(self.dropout_data, data)  # changed by nicktrejo

    def saveDropoutFromCsv(self, f_drop_data_name, course, run) -> None:
        """
        Save multiple values for users dropout taken from csv in
        corresponding collection (dropout_data)

        :param f_drop_data_name: csv file with users and their grades
        :type f_drop_data_name: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        with open(f_drop_data_name, 'r') as f_drop:  # changed by nicktrejo
            reader = csv.DictReader(f_drop, delimiter=';')  # changed by nicktrejo
            reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]  # changed by nicktrejo

            dropouts = [[row['user_id'], row['status'], row['last_connection'], row['number_activity_days'], row['dropout']] for row in reader]  # changed by nicktrejo

        args = []
        for drop in dropouts:
            args.append((self.getCourserunUserId(drop[0], course, run), drop[1], drop[2], drop[3], drop[4]))  # changed by nicktrejo

        self.insertMultiple(self.dropout_data, args)  # changed by nicktrejo

    def saveTypeFromCsv(self, cert_file, course, run) -> None:
        """
        Save multiple values for certificates and users taken from csv in
        corresponding collection (dropout) (is it ok?)
        (similar to saveCertificatesFromCsv)
        Deprecated: Currently not used (apparently)

        :param cert_file: csv file with users and their grades - certificates
        :type cert_file: str
        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        with open(cert_file, 'r') as f_type:  # changed by nicktrejo
            reader = csv.DictReader(f_type, delimiter=';')  # changed by nicktrejo
            reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]  # changed by nicktrejo

            dropouts = [[row['user_id'], row['grade'], row['status']] for row in reader]  # changed by nicktrejo

        for drop in dropouts:
            data = (drop[2], self.getCourserunUserId(drop[0], course, run))  # changed by nicktrejo
            # test = self.db.dropout.find_one({'courserun_user_id':data[1]})  # variable not used
            self.db.dropout.update({'courserun_user_id':data[1]}, {'$set':{'type': data[0]}})

    ########################################
    # Course Run Attributes

    ####################
    # Course - Indicator names
    def saveCourseIndicatorName(self, course, run, indicator) -> None:
        """
        Save single value for indicator name in corresponding collection
        (course_indicator_names_data)

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param indicator: indicator's name
        :type indicator: str
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        data = (course_run_id, indicator)
        self.insertStatement(self.course_indicator_names_data, data)  # changed by nicktrejo


    def saveCourseIndicatorNames(self, course, run, indicators) -> None:
        """
        Save multiple values for indicators name in corresponding collection
        (course_indicator_names_data)

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param indicators: list of indicators' name
        :type indicators: list
        """
        for indicator in indicators:
            self.saveCourseIndicatorName(course, run, indicator)

    def getIndicatorNames(self, course, run) -> None:
        """
        Get all indicators' name for `course` and `run`
        Method not implemented yet

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        """
        pass


    ####################
    # Course - Duration
    def saveCourseDuration(self, course, run, duration) -> None:
        """
        Save single value for duration in corresponding collection (course_duration_data)

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param duration: duration for the course
        :type duration: int  # isn't it?
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        data = (course_run_id, duration)
        self.insertStatement(self.course_duration_data, data)  # changed by nicktrejo

    ####################
    # Course - Prediction meth
    def saveCoursePredictionMeth(self, course, run, meth) -> None:
        """
        Save single value for prediction method name in corresponding collection
        (course_prediction_meths_names_data)

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param meth: method name
        :type meth: str
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        data = (course_run_id, meth)
        self.insertStatement(self.course_prediction_meths_names_data, data)  # changed by nicktrejo

    def saveCoursePredictionMeths(self, course, run, meths) -> None:
        """
        Save multiple value for prediction method name in corresponding collection
        (course_prediction_meths_names_data)

        :param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param meths: list of methods name
        :type meths: list
        """
        for meth in meths:
            self.saveCoursePredictionMeth(course, run, meth)


    ####################
    # Course - Output
    def saveCourseOutput(self, course, run, output) -> None:
        """
        Save single value for output (?) in corresponding collection (course_outputs_data)

        ::param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param output: output??
        :type output: str  # isn't it?
        """
        course_run_id = self.getCourseRunId(course, run)  # changed by nicktrejo
        data = (course_run_id, output)
        self.insertStatement(self.course_outputs_data, data)  # changed by nicktrejo

    def saveCourseOutputs(self, course, run, outputs) -> None:
        """
        Save multiple values for outputs (?) in corresponding collection (course_outputs_data)

        ::param course: course_id given by edX
        :type course: str
        :param run: version of the course
        :type run: str
        :param outputs: list of output??
        :type outputs: list
        """
        for output in outputs:
            self.saveCourseOutput(course, run, output)


    def updateCourseStructure(self, course_id, problems) -> None:
        """
        Save (insert or update) multiple values for problem info in corresponding
        collection (course_structure)

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :param problems: list of problems info as dictionaries
        :type problems: list
        """
        if self.db.course_structure.count_documents({'course_id' : course_id}) == 0:
            coll = self.db[self.course_structure['coll']]
            coll.insert_many(problems)
        else:
            for problem in problems:
                query = {'course_id' : course_id, 'problem_id' : problem['problem_id']}
                if self.db.course_structure.count_documents(query) == 0:
                    self.db.course_structure.insert_one(problem)
                else:
                    self.db.course_structure.update_one(query, {'$set':problem})

    def getCourseStructure(self, course_id) -> list:
        """
        Get list of problems for course_id (course_structure)

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: list of problems for course (course_structure)
        :rtype: list
        """
        course_structure = self.db.course_structure.find({'course_id':course_id})
        return [problem for problem in course_structure]

    #Comes from a built up data structure, no need to build here
    def saveFinalIndicators(self, indicators) -> None:
        """
        Save multiple values for indicators in corresponding collection (final_indicators)
        It also creates an index with 'dt_date' if it doesn't exist

        :param indicators: structured info for indicators
        :type indicators: list, pandas.core.frame.DataFrame
        """
        coll = self.db[self.final_indicators['coll']]
        coll.insert_many(indicators)
        coll.create_index([('dt_date', 1)])  # Added by nicktrejo

    def saveSeenVideos(self, indicators) -> None:
        """
        Save multiple values for seen videos in corresponding collection (seen_videos)
        It also creates an index with 'dt_date' if it doesn't exist

        :param indicators: structured info for seen videos
        :type indicators: list, pandas.core.frame.DataFrame
        """
        coll = self.db[self.seen_videos['coll']]
        coll.insert_many(indicators)
        coll.create_index([('dt_date', 1)])  # Added by nicktrejo

    def saveSeenProblems(self, indicators) -> None:
        """
        Save multiple values for seen problems in corresponding collection (seen_problems)
        It also creates an index with 'dt_date' if it doesn't exist

        :param indicators: structured info for seen problems
        :type indicators: list, pandas.core.frame.DataFrame
        """
        coll = self.db[self.seen_problems['coll']]
        coll.insert_many(indicators)
        coll.create_index([('dt_date', 1)])  # Added by nicktrejo

    def saveEventTrack(self, indicators) -> None:
        """
        Save multiple values for event track in corresponding collection (event_track)

        :param indicators: structured info for seen event track
        :type indicators: list, pandas.core.frame.DataFrame
        """
        coll = self.db[self.event_track['coll']]
        coll.insert_many(indicators)


    def getCourseInfo(self, course_id) -> dict:
        """
        Get course info, and last log date (from final_indicators)

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: course info including last_log_date
        :rtype: dict, bool
        """
        query = {'course_id':course_id}
        course_info = self.db.course_info.find_one(query)
        if course_info is None:
            return False  # TODO nicktrejo: i think better is 'return None'

        num_records = self.db.final_indicators.count_documents(query)
        if num_records > 0:
            course_info['last_log_date'] = self.db.final_indicators.find().sort([('dt_date', -1)]).limit(1).next().get('dt_date')
        else:
            course_info['last_log_date'] = datetime.min
        return course_info

    def getCourseUsers(self, course_id) -> list:
        """
        Get all users profiles for course_id in course_users collection

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: list of users' profiles
        :rtype: list
        """
        query = {'course_id' : course_id}
        course_users = self.db.course_users.find(query)
        return [course_user for course_user in course_users]


    def updateUsers(self, course_id, users) -> None:
        """
        Save (insert or update) multiple values for users profile info in
        corresponding collection (course_users)

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :param users: users' profiles from edX
        :type users: list, pandas.core.frame.DataFrame
        """
        query = {'course_id' : course_id}
        if self.db.course_users.count_documents(query) == 0:
            coll = self.db[self.course_users['coll']]
            coll.insert_many(users)
            return
        for user in users:
            query['user_id'] = user['user_id']
            if self.db.course_users.count_documents(query) == 0:
                self.db.course_users.insert_one(user)
            else:
                self.db.course_users.update_one(query, {'$set':user})

    def getFinalIndicatorsAggrData(self, course_id):
        pipeline = [
            {"$match": {"course_id" : course_id}},
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "course_id": "$course_id"
                    },
                    "user_id": {"$first" : "$user_id"},
                    "course_id": {"$first" : "$course_id"},
                    "num_events": {"$max": '$num_events'},
                    "num_sessions": {"$max": '$num_sessions'},
                    "video_time": {"$max": '$video_time'},
                    "problem_time": {"$max": '$problem_time'},
                    "nav_time": {"$max": '$nav_time'},
                    "forum_time": {"$max": '$forum_time'},
                    "total_time": {"$max": '$total_time'},
                    "forum_events": {"$max": '$forum_events'},
                    "nav_events": {"$max": '$nav_events'},
                    "problem_events": {"$max": '$problem_events'},
                    "video_events": {"$max": '$video_events'},
                    "consecutive_inactivity_days": {"$max": '$consecutive_inactivity_days'},
                    "connected_days": {"$max": '$connected_days'},
                    "different_videos": {"$max": '$different_videos'},
                    "different_problems": {"$max": '$different_problems'}
                }
            },
            {
                "$lookup" : {
                    "from": "course_users",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "users",
                },
            },
            {"$unwind" : "$users"},
            #{
            #   "$lookup" : {
            #       "from": "seen_videos",
            #       "let": {
            #           "userId": "$user_id",
            #           "courseId": "$course_id"
            #       },
            #       "pipeline": [
            #       {
            #           "$match": {
            #               "$expr": {
            #                   "$and": [
            #                   {
            #                       "$eq": ["$user_id", "$$userId"]
            #                   },
            #                   {
            #                       "$eq": ["$course_id", "$$courseId"]
            #                   },
            #                   {
            #                       "$eq": ["$course_id", course_id]
            #                   }
            #                   ]
            #               }
            #           }
            #       }
            #       ],
            #       "as": "user_videos"
            #   },
            #},
            #{
            #   "$lookup" : {
            #       "from": "seen_problems",
            #       "let": {
            #           "userId": "$user_id",
            #           "courseId": "$course_id"
            #       },
            #       "pipeline": [
            #       {
            #           "$match": {
            #               "$expr": {
            #                   "$and": [
            #                   {
            #                       "$eq": ["$user_id", "$$userId"]
            #                   },
            #                   {
            #                       "$eq": ["$course_id", "$$courseId"]
            #                   },
            #                   {
            #                       "$eq": ["$course_id", course_id]
            #                   },
            #                   ]
            #               }
            #           }
            #       }
            #       ],
            #       "as": "user_problems"
            #   },
            #},
            {
                "$project": {
                    "_id" : 0,
                    "username": "$users.username",
                    "lower_username": {"$toLower": "$users.username"},
                    "user_id": 1,
                    "num_events": 1,
                    "num_sessions": 1,
                    "video_time": 1,
                    "problem_time": 1,
                    "nav_time": 1,
                    "forum_time": 1,
                    "total_time": 1,
                    "forum_events": 1,
                    "nav_events": 1,
                    "problem_events": 1,
                    "video_events": 1,
                    "consecutive_inactivity_days": 1,
                    "connected_days": 1,
                    "different_videos": 1,
                    "different_problems": 1,
                    #"different_problems": {"$toDouble" : {"$size": "$user_problems"}},
                    #"different_videos": {"$toDouble" : {"$size": "$user_videos"}},
                }
            },
            {
                "$sort": {
                    "lower_username" : 1,
                }
            },
            {
                "$project": {
                    "lower_username": 0,
                }
            },
        ]
        data_cursor = self.db.final_indicators.aggregate(pipeline)
        return [data for data in data_cursor]

    def getSeenVideos(self, course_id) -> list:
        """
        Get seen videos info sorted by date for course_id in seen_videos collection

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: list of seen videos info sorted by date
        :rtype: list
        """
        seen_videos = self.db.seen_videos.find({'course_id' : course_id}).sort([('dt_date', 1)])
        return [seen_video for seen_video in seen_videos]

    def getSeenProblems(self, course_id) -> list:
        """
        Get seen problems info sorted by date for course_id in seen_problems collection

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: list of seen problems info sorted by date
        :rtype: list
        """
        seen_problems = self.db.seen_problems.find({'course_id' : course_id}).sort([('dt_date', 1)])
        # if index is not previously set, this may fail in RAM
        return [seen_problem for seen_problem in seen_problems]  # list(seen_problems)

    def getSeenVideosAggrData(self) -> list:
        """
        Get seen video info aggregated, grouped by user & course with
        total number of seen videos (from seen_videos collection)
        (sorted by number of seen videos)

        :return: list of users and total number of seen videos
        :rtype: list
        """
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "course_id": "$course_id",
                    },
                    "user_id": {"$first": "$user_id"},
                    "course_id": {"$first": "$course_id"},
                    "num_videos": {"$sum": 1},
                }
            },
            {
                "$sort": {"num_videos" : 1},
            }
        ]
        data_cursor = self.db.seen_videos.aggregate(pipeline)
        return [data for data in data_cursor]

    def getSeenProblemsAggrData(self) -> list:
        """
        Get seen problems info aggregated, grouped by user & course with
        total number of seen videos (from seen_problems collection)

        :return: list of users and total number of different problems
        :rtype: list
        """
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "course_id": "$course_id",
                    },
                    "user_id": {"$first": "$user_id"},
                    "course_id": {"$first": "$course_id"},
                    "diff_problems": {"$sum": 1},
                },
            }
        ]
        data_cursor = self.db.seen_problems.aggregate(pipeline)
        return [data for data in data_cursor]

    def getEventTrackAggrData(self) -> list:
        """
        Get event track info aggregated, grouped by user, course and
        type_summary with total number of problems (?) (from event_track collection)
        (sorted by user_id, type_summary)

        :return: list of users and total number of problems (?)
        :rtype: list
        """
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "user_id": "$user_id",
                        "course_id": "$course_id",
                        #"time_day": "$time_day",
                        "type_summary": "$type_summary"
                    },
                    "user_id": {"$first": "$user_id"},
                    "course_id": {"$first": "$course_id"},
                    #"time_day": "$time_day",
                    "type_summary": {"$first": "$type_summary"},
                    "num_problems": {"$sum": 1},
                }
            },
            {
                "$sort": {
                    "_id.user_id" : 1,
                    "_id.type_summary" : 1,
                    #"_id.time_day" : 1
                }
            },
            {"$project": {"_id" : 0}},
        ]
        data_cursor = self.db.event_track.aggregate(pipeline)
        return [data for data in data_cursor]

    def getFinalIndicatorsData(self, course_id) -> list:
        """
        Get final indicators info for all users (from final_indicators collection)
        (sorted by username, dt_date)

        :return: list of users and total number of problems (?)
        :rtype: list
        """
        pipeline = [
            {
                "$match": {
                    "course_id" : course_id
                }
            },
            {
                "$lookup" : {
                    "from": "course_users",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "users",
                },
            },
            {"$unwind" : "$users"},
            {
                "$project": {
                    "_id" : 0,
                    "lower_username": {"$toLower": "$users.username"},
                    "user_id": 1,
                    "dt_date": 1,  # changed by nicktrejo
                    "num_events": 1,
                    "num_sessions": 1,
                    "video_time": 1,
                    "problem_time": 1,
                    "nav_time": 1,
                    "forum_time": 1,
                    "enrollment_time": 1,  # added by nicktrejo
                    "total_time": 1,
                    "enrollment_events": 1,  # added by nicktrejo
                    "forum_events": 1,
                    "nav_events": 1,
                    "problem_events": 1,
                    "video_events": 1,
                    "consecutive_inactivity_days": 1,
                    "connected_days": 1,
                    "different_videos": 1,
                    "different_problems": 1,
                    "enrollment_mode": 1,  # added by nicktrejo
                }
            },
            {
                "$sort": {
                    "lower_username" : 1,
                    "dt_date" : 1,
                }
            },
            {
                "$project": {
                    "lower_username": 0,
                }
            },
        ]
        data_cursor = self.db.final_indicators.aggregate(pipeline, allowDiskUse=True)
        return [data for data in data_cursor]

    def updateConnectedDaysTime(self, course_id) -> None:
        """
        TODO: DESCRIPTION MISSING

        :param course_id: course_id given by edX (?)
        :type course_id: str
        """
        indicators = self.getFinalIndicatorsData(course_id)
        last_user_id = -1
        for indicator in indicators:
            user_id = indicator['user_id']
            if user_id != last_user_id:
                connected_days = 0
                consecutive_inactivity_days = 0
                last_user_id = user_id
                print('Next User_id: ', user_id)
            if self.checkIndicatorActivity(indicator):
                connected_days += 1
                consecutive_inactivity_days = 0
            else:
                consecutive_inactivity_days += 1

            indicator['connected_days'] = connected_days
            indicator['consecutive_inactivity_days'] = consecutive_inactivity_days
            query = {'course_id' : course_id, 'user_id' : indicator['user_id'], 'time_day' : indicator['time_day']}
            self.db.final_indicators.update_one(query, {'$set':indicator})

    def checkIndicatorActivity(self, indicator) -> bool:
        """
        TODO: DESCRIPTION MISSING

        :param indicator: TODO: complete
        :type indicator: TODO: complete
        :return: TODO: complete
        :rtype: bool
        """
        if indicator['num_events'] == 0 and indicator['total_time'] == 0:
            return False
        else:  # Unnecessary "else" after "return"
            return True

    def getUserLastSeenInfo(self, user_id, course_id) -> tuple:
        """
        TODO: DESCRIPTION MISSING

        :param user_id: user_id given by edX (?) TODO: check
        :type user_id: str
        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: TODO: complete
        :rtype: tuple
        """
        cursor = self.db.final_indicators.find({'user_id' : user_id, 'course_id' : course_id}).sort([('dt_date', -1)]).limit(1)
        if cursor.count() == 0:
            return 0, 0
        else:  # Unnecessary "else" after "return"
            indicator = cursor.next()
            return indicator['different_videos'], indicator['different_problems']


    def getUserLastActivityInfo(self, user_id, course_id) -> tuple:
        """
        TODO: DESCRIPTION MISSING

        :param user_id: user_id given by edX (?) TODO: check
        :type user_id: str
        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: TODO: complete
        :rtype: tuple
        """
        cursor = self.db.final_indicators.find({'user_id' : user_id, 'course_id' : course_id}).sort([('time_day', -1)]).limit(1)
        # The following should be better (nicktrejo)
        # cursor = self.db.final_indicators.find_one({'user_id': user_id, 'course_id': course_id}, sort=[('time_day', -1)])
        # if not cursor --> return 0, 0
        if cursor.count() == 0:
            return 0, 0
        indicator = cursor.next()
        return indicator['consecutive_inactivity_days'], indicator['connected_days']

    def importCollection(self, info, course_id, collection_name) -> None:
        """
        TODO: DESCRIPTION MISSING

        :param info: TODO: complete
        :type info: TODO: complete
        :param course_id: course_id given by edX (?)
        :type course_id: str
        :param collection_name: TODO: complete
        :type collection_name: TODO: complete
        """
        query = {'course_id' : course_id}
        if self.db[collection_name].count_documents(query) > 0:
            print('WARNING: Collection {} is not empty. The insertion will not take place'.format(collection_name))
        else:
            self.db[collection_name].insert(info)

    def insertCourseInfo(self, course_name, course_id) -> None:
        """
        TODO: DESCRIPTION MISSING

        :param course_name: TODO: complete
        :type course_name: TODO: complete
        :param course_id: course_id given by edX (?)
        :type course_id: str
        """
        query = {'course_id' : course_id}
        if self.db.course_info.count_documents(query) == 0:
            self.db.course_info.insert_one({'course_name' : course_name, 'course_id' : course_id})
        else:
            self.db.course_info.update_one(query, {'$set':{'course_name': course_name}})

    ####################

    def getCurrentInactivityDays(self, course_id) -> list:
        """
        TODO: DESCRIPTION MISSING

        :param course_id: course_id given by edX (?)
        :type course_id: str
        :return: TODO: complete
        :rtype: list
        """
        pipeline = [
            {
                "$match": {
                    "course_id" : course_id
                }
            },
            {
                "$sort": {
                    "user_id" : 1,
                    "time_day" : -1,
                }
            },
            {
                "$group": {
                    "_id": {"user_id": "$user_id"},
                    "user_id": {"$first" : "$user_id"},
                    "consecutive_inactivity_days": {"$first" : "$consecutive_inactivity_days"},
                    #"time_day": {"$first" : "$time_day"},
                    #"_id": {
                    #   "user_id": "$user_id",
                    #   "consecutive_inactivity_days": {"$first" : "$consecutive_inactivity_days"},
                    #},
                    #"user_id": {"$first" : "$user_id"},
                    #"course_id": {"$first" : "$course_id"},
                    #"time_day": {"$first" : "$time_day"},
                    #"num_videos": {"$sum": 1 },
                }
            },
            {
                "$project": {
                    "_id": 0,
                }
            },

        ]

        data_cursor = self.db.final_indicators.aggregate(pipeline, allowDiskUse=True)
        return [data for data in data_cursor]

    # close conection
    def closeConection(self) -> None:
        """
        Close client connection to database

        """
        self.client.close()


# test
if __name__ == '__main__':

    # It will fail because of missing required arguments
    edx = EdxMongoStore()

    BASE_PATH = Path('C:/Users/Juan/Desktop/edX-istaladores/edx-mas_mongo/data/DatosAnonimizados/')  # changed by nicktrejo

    courses = [
        #{'course':'Quijote501x', 'run':'3T2016', 'path': 'Quijote/Quijote501x_2016_3T/certificate/CertificateQuijote501x3T2016.csv'},  # changed by nicktrejo
        #{'course':'Quijote501x', 'run':'3T2015', 'path': 'Quijote/Quijote501x_2015_3T/certificate/CertificateQuijote501x3T2015.csv'},  # changed by nicktrejo
        #{'course':'Quijote501x', 'run':'1T2015', 'path': 'Quijote/Quijote501x_2015_1T/certificate/CertificateQuijote501x1T2015.csv'},  # changed by nicktrejo
        #{'course':'Renal701x', 'run':'3T2016', 'path': 'Renal/Renal701x_2016_3T/certificate/CertificateRenal701x3T2016.csv'},  # changed by nicktrejo
        #{'course':'Renal701x', 'run':'1T2016', 'path': 'Renal/Renal701x_2016_1T/certificate/CertificateRenal701x1T2016.csv'},  # changed by nicktrejo
        {'course':'Equidad801x', 'run':'1T2016', 'path': 'Equidad801x_2016_1T/certificate/CertificateEquidad801x1T2016.csv'},  # changed by nicktrejo
        #{'course':'Equidad801x', 'run':'3T2016', 'path': 'Equidad/Equidad801x_2016_3T/certificate/CertificateEquidad801x3T2016.csv'},  # changed by nicktrejo
        #{'course': 'Idealismo501x', 'run': '1T2016', 'path' : 'Idealismo/Idealismo501x_2016_1T/certificate/CertificateIdealismo501x1T2016.csv'}  # changed by nicktrejo
    ]

    for course in courses:
        edx.saveTypeFromCsv(BASE_PATH / course['path'], course['course'], course['run'])  # changed by nicktrejo
