# -*- coding: utf-8 -*-

"""
Project: TFG edX-MAS
Author: Victor Macias Palla
Modified: Juan Soberon Higuera
Date last modified: September 2019

Module for insert data in Mongo store
"""

import csv
from pymongo import MongoClient
import urllib.parse
import json
from datetime import datetime
import pandas as pd # TO BE ABLE TO USE DATAFRAMES

class EdxMongoStore():

	def __init__(self, host, port, dbname, username = None, pwd = None):
		self.connect(host, port, dbname, username, pwd)
		self.course_runs_data = {'coll':'course_runs', 'attributes':['course', 'run']}
		self.course_users_data = {'coll':'course_users', 'attributes':['course_run_id', 'username']}
		self.indicators_data = {'coll':'indicators', 'attributes':['courserun_user_id', 'indicator_name', 'value', 'day']}
		self.certificates_data = {'coll':'certificates', 'attributes':['courserun_user_id', 'grade', 'status']}
		self.course_indicator_names_data = {'coll':'course_indicator_names', 'attributes':['courserun_user_id', 'indicator_name']}
		self.course_duration_data = {'coll':'course_duration', 'attributes':['course_run_id', 'duration']}
		self.course_prediction_meths_names_data = {'coll':'course_prediction_meths_names', 'attributes':['course_run_id', 'meth']}
		self.course_outputs_data = {'coll':'course_outputs', 'attributes':['course_run_id', 'output']}
		self.dropout_data = {'coll':'dropout', 'attributes':['courserun_user_id', 'type', 'last_connection', 'number_activity_days', 'status']}
		self.course_structure = {'coll':'course_structure', 'attributes':['course_id','seq_block_txt', 'problem_id', 'problem_wieght', 'problem_max_attemps']}
		self.final_indicators = {'coll':'final_indicators', 'attributes':['course_id', 'user_id', 'day', 'num_events', 'num_sessions', 'video_time', 'problem_time', 'nav_time', 'forum_time', 'total_time', 'forum_events', 'nav_events', 'video_events', 'problem_events', 'consecutive_inactivity_days', 'connected_days', 'different_videos', 'different_problems']}
		self.course_info = {'coll':'course_info', 'attributes':['course_id', 'course_name']}
		self.course_users = {'coll':'course_users', 'attributes':['user_id', 'course_id', 'username', 'name', 'email', 'language', 'location', 'year_of_birth', 'gender', 'level_of_education', 'mailing_address', 'goals', 'enrollment_mode', 'verification_status', 'cohort', 'city', 'country']}
		self.seen_videos = {'coll':'seen_videos', 'attributes':['course_id', 'user_id', 'time_day', 'video_id', 'video_code']}
		self.seen_problems = {'coll':'seen_problems', 'attributes':['course_id', 'user_id', 'time_day', 'problem_id', 'block_info', 'grade', 'max_grade', 'nAttemps']}
		self.event_track = {'coll':'event_track', 'attributes':['course_id', 'user_id', 'event_type', 'type_summary', 'time_day', 'time_hourDetail']}
        
	def connect(self, host, port, dbname, username = None, pwd = None):
		db_host = host
		db_port = port
		db_name = dbname
		if db_host == 'localhost' or db_host == '127.0.0.1':
			self.client = MongoClient(db_host, db_port)
		else:
			db_username = urllib.parse.quote_plus(username)
			db_password = urllib.parse.quote_plus(pwd)
			self.client = MongoClient('mongodb://%s:%s@%s:%s' % (db_username, db_password, db_host, db_port))
		self.db = self.client[db_name]

	def build_mongo_one(self, attributes, data):
		obj = {}
		for i in zip(attributes, data):
			obj[i[0]] = i[1]
		return obj
	def build_mongo_many(self, attributes, data):
		array = [self.build_mongo_one(attributes, data_obj) for data_obj in data]
		return array

	def insert_statement(self, info, data):
		coll = self.db[info['coll']]
		new_data = self.build_mongo_one(info['attributes'], data)
		coll.insert_one(new_data)

	def insert_multiple(self, info, data):
		coll = self.db[info['coll']]
		new_data = self.build_mongo_many(info['attributes'], data)
		coll.insert_many(new_data)
		
	def select_statement(self, info, data):
		coll = self.db[info['coll']]
		result = coll.find_one(data)
		return result

	########################################
	# Course - Run
	def saveCourse(self, course, run):
		data = (course, run)
		self.insert_statement(self.course_runs_data, data)

	def get_course_run_id(self, course, run):
		attrs = {'course': course, 'run':run}
		result = self.select_statement(self.course_runs_data, attrs)
		return result.get('_id')
		
	########################################
	# Course - Run - Users
	def saveUserInCourse(self, username, course, run):
		'''
		course_run_id = self.get_course_run_id(course, run)
		query = self.insert_query_course_users + "(%s, %s)"
		data  = (course_run_id, username)
		self.insert_statement(query, data)
		'''
		course_run_id = self.get_course_run_id(course, run)
		data  = (course_run_id, username)
		self.insert_statement(self.course_users_data, data)

	def get_courserun_user_id(self, username, course, run):
		#test = self.db.course_users.aggregate([
			#{'$lookup': {
			#	'from': "course_runs",
			#	'localField': "course_run_id",
			#	'foreignField': "_id",
			#	'as': "course_runs_users"
			#	}},			
		    #{'$match' : {'$and': [{"course_runs_users.username" : username},{"course_runs_users.course" : course},{"course_runs_users.run": run}]}}

		    
		     #{"course_runs_users.username" : username, "course_runs_users.course": course, "course_runs_users.run": run}},
		#    {$project : {
	    #        posts : { $filter : {input : "$posts"  , as : "post", cond : { $eq : ['$$post.via' , 'facebook'] } } },
	    #        admin : 1

	    #   }}
		#])
		course_run_id = self.db.course_runs.find_one({'course':course, 'run':run}).get('_id')
		course_run_user_id = self.db.course_users.find_one({'course_run_id':course_run_id, 'username':username}).get('_id')
		return course_run_user_id

		'''
		query = """ SELECT courserun_user_id
		            FROM course_users NATURAL JOIN course_runs
		            WHERE username=%s and course=%s and run=%s
		        """
		data = (username, course, run)

		result = self.select_sql_statement(query, data)
		courserun_user_id_list = [courseuser[0] for courseuser in result]
		return courserun_user_id_list[0]
		'''

	def getUsersInCourse(self, course, run):
		
		course_run_id = self.db.course_runs.find_one({'course':course, 'run':run}).get('_id')
		#course_run_users = self.db.course_users.find({'course_run_id':course_run_id}).toArray()
		course_run_users = self.db.course_users.find({'course_run_id':course_run_id})
		users = [user['username'] for user in course_run_users]
		return users

	def saveUsersFromCsv(self, f_users_data_name, course, run):
		f_users = open(f_users_data_name, "r")
		reader  = csv.DictReader(f_users, delimiter=';')
		reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]

		users = [row['user_id'] for row in reader]

		f_users.close()
		course_run_id = self.get_course_run_id(course, run)
		args  = [(course_run_id, user) for user in users]
		self.insert_multiple(self.course_users_data, args)

	########################################
	# Indicators
	def saveUserIndicator(self, course, run, indicator, username, value, day):
		courserun_user_id = self.get_courserun_user_id(username, course, run)
		data = (courserun_user_id, indicator, value, day)
		self.insert_statement(self.indicators_data, data)

	def saveIndicatorsFromDict(self, users, dict_events, indicator_event_list, indicator_activity_list):

		if indicator_event_list:
			course = indicator_event_list[0].course
			run    = indicator_event_list[0].run
			day    = indicator_event_list[0].day
		elif indicator_activity_list:
			course = indicator_activity_list[0].course
			run    = indicator_activity_list[0].run
			day    = indicator_activity_list[0].day
		else:
			return		

		#query = self.insert_query_indicators
		args = []

		for user in dict_events:
			user_events = dict_events[user]
			courserun_user_id = self.get_courserun_user_id(user, course, run)
			for ind in indicator_event_list:
				value = ind.getIndicator(user_events)
				args.append((courserun_user_id, ind.indicator, value, day))

		for user in users:
			courserun_user_id = self.get_courserun_user_id(user, course, run)
			for ind in indicator_activity_list:
				value = ind.getIndicator(user, day)
				args.append((courserun_user_id, ind.indicator, value, day))

		self.insert_multiple(self.indicators_data, args)

	########################################
	# Certificates
	def saveUserCertificate(self, username, grade, status, course, run):
		status = 1 if status == 'downloadable' else 0

		courserun_user_id = self.get_courserun_user_id(username, course, run)
		data  = (courserun_user_id, grade, status)

		self.insert_statement(self.certificates_data, data)

	def saveCertificatesFromCsv(self, f_cert_data_name, course, run):
		f_cert = open(f_cert_data_name, "r")
		reader = csv.DictReader(f_cert, delimiter=';')
		reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]

		certs  = [[row['user_id'], row['grade'], row['status']] for row in reader]

		f_cert.close()

		args  = [(self.get_courserun_user_id(cert[0], course, run), cert[1], 0 if cert[2] in ["notpassing","audit_notpassing"] else 1) for cert in certs]
		self.insert_multiple(self.certificates_data, args)

	########################################
	# Dropout
	def saveUserDropout(self, username, mark_type, last_connection, number_activity_days, dropout, course, run):
		courserun_user_id = self.get_courserun_user_id(username, course, run)
		data  = (courserun_user_id, mark_type, last_connection, number_activity_days, dropout)

		self.insert_statement(self.dropout_data, data)

	def saveDropoutFromCsv(self, f_drop_data_name, course, run):
		f_drop = open(f_drop_data_name, "r")
		reader = csv.DictReader(f_drop, delimiter=';')
		reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]

		dropouts  = [[row['user_id'], row['status'], row['last_connection'], row['number_activity_days'], row['dropout']] for row in reader]

		f_drop.close()

		args = []
		for drop in dropouts:
			args.append((self.get_courserun_user_id(drop[0], course, run), drop[1], drop[2], drop[3], drop[4]))
		
		self.insert_multiple(self.dropout_data, args)

	def saveTypeFromCsv(self, cert_file, course, run):
		f_type = open(cert_file, "r")
		reader = csv.DictReader(f_type, delimiter=';')
		reader.fieldnames = [field.strip().lower() for field in reader.fieldnames]

		dropouts = [[row['user_id'], row['grade'], row['status']] for row in reader]

		f_type.close()
		for drop in dropouts:
			data = (drop[2], self.get_courserun_user_id(drop[0], course, run))
			test = self.db.dropout.find_one({'courserun_user_id':data[1]})
			self.db.dropout.update({'courserun_user_id':data[1]}, {'$set':{'type': data[0]}})

	########################################
	# Course Run Attributes

	####################
	# Course - Indicator names
	def saveCourseIndicatorName(self, course, run, indicator):
		course_run_id = self.get_course_run_id(course, run)
		data = (course_run_id, indicator)
		self.insert_statement(self.course_indicator_names_data, data)


	def saveCourseIndicatorNames(self, course, run, indicators):
		for indicator in indicators:
			self.saveCourseIndicatorName(course, run, indicator)

	def getIndicatorNames(self, course, run):
		pass


	####################
	# Course - Duration
	def saveCourseDuration(self, course, run, duration):
		course_run_id = self.get_course_run_id(course, run)
		data = (course_run_id, duration)
		self.insert_statement(self.course_duration_data, data)
	
	####################
	# Course - Prediction meth
	def saveCoursePredictionMeth(self, course, run, meth):
		course_run_id = self.get_course_run_id(course, run)
		data = (course_run_id, meth)
		try:
			self.insert_statement(self.course_prediction_meths_names_data, data)
		except psycopg2.IntegrityError:
			self.con.rollback()

	def saveCoursePredictionMeths(self, course, run, meths):
		for meth in meths:
			self.saveCoursePredictionMeth(course, run, meth)


	####################
	# Course - Output
	def saveCourseOutput(self, course, run, output):
		course_run_id = self.get_course_run_id(course, run)
		data = (course_run_id, output)
		try:
			self.insert_statement(self.course_outputs_data, data)
		except psycopg2.IntegrityError:
			self.con.rollback()

	def saveCourseOutputs(self, course, run, outputs):
		for output in outputs:
			self.saveCourseOutput(course, run, output)


	def updateCourseStructure(self, course_id, problems):
		if (self.db.course_structure.count_documents({'course_id' : course_id}) == 0):
			coll = self.db[self.course_structure['coll']]
			coll.insert_many(problems)
		else:
			for problem in problems:
				query = {'course_id' : course_id, 'problem_id' : problem['problem_id']}
				if (self.db.course_structure.count_documents(query) == 0):
					self.db.course_structure.insert_one(problem)
				else:
					self.db.course_structure.update_one(query, {'$set':problem})

	def getCourseStructure(self, course_id):#, sort_idx=1, sort_ord=1):
		course_structure = self.db.course_structure.find({'course_id':course_id})#.sort(sort_idx, sort_ord)		
		return [problem for problem in course_structure]

	#Comes from a built up data structure, no need to build here
	def saveFinalIndicators(self, indicators):
		coll = self.db[self.final_indicators['coll']]
		coll.insert_many(indicators)

	def saveSeenVideos(self, indicators):
		coll = self.db[self.seen_videos['coll']]
		coll.insert_many(indicators)

	def saveSeenProblems(self, indicators):
		coll = self.db[self.seen_problems['coll']]
		coll.insert_many(indicators)

	def saveEventTrack(self, indicators):
		coll = self.db[self.event_track['coll']]
		coll.insert_many(indicators)


	def getCourseInfo(self, course_id):
		course_info = self.db.course_info.find_one({'course_id':course_id})
		if course_info is None:
			return False
		else:
			num_records = self.db.final_indicators.count_documents({'course_id' : course_id})
			#num_records = self.db.final_indicators.count_documents({'course_id' : course_id})
			if (num_records > 0):
				course_info['last_log_date'] = self.db.final_indicators.find().sort([('time_day',-1)]).limit(1).next().get('time_day')
			else: 
				course_info['last_log_date'] = datetime.min
			return course_info

	def getCourseUsers(self, course_id):
		course_users = self.db.course_users.find({'course_id' : course_id})		
		return [course_user for course_user in course_users]

	
	def updateUsers(self, course_id, users):
		if (self.db.course_users.count_documents({'course_id' : course_id}) == 0):
			coll = self.db[self.course_users['coll']]
			coll.insert_many(users)
		else:
			for user in users:
				query = {'course_id' : course_id, 'user_id' : user['user_id']}
				if (self.db.course_users.count_documents(query) == 0):
					self.db.course_users.insert_one(user)
				else:
					self.db.course_users.update_one(query, {'$set':user})

	def getFinalIndicatorsAggrData(self, course_id):
		pipeline = [
			{"$match": {"course_id" : course_id,}},
			{
				"$group": {
					"_id": {
						"user_id": "$user_id",
						"course_id": "$course_id"
					},
					"user_id": {"$first" : "$user_id"},
					"course_id": {"$first" : "$course_id"},
					"num_events": { "$max": '$num_events' },
					"num_sessions": { "$max": '$num_sessions' },
					"video_time": { "$max": '$video_time' },
					"problem_time": { "$max": '$problem_time' },
					"nav_time": { "$max": '$nav_time' },
					"forum_time": { "$max": '$forum_time' },
					"total_time": { "$max": '$total_time' },
					"forum_events": { "$max": '$forum_events' },
					"nav_events": { "$max": '$nav_events' },
					"problem_events": { "$max": '$problem_events' },
					"video_events": { "$max": '$video_events' },
					"consecutive_inactivity_days": { "$max": '$consecutive_inactivity_days' },
					"connected_days": { "$max": '$connected_days' },
					"different_videos": { "$max": '$different_videos' },
					"different_problems": { "$max": '$different_problems' }
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
			#	"$lookup" : {
			#		"from": "seen_videos",
			#		"let": {
			#			"userId": "$user_id",
			#			"courseId": "$course_id"
			#		},
			#		"pipeline": [
			#		{
			#			"$match": {
			#				"$expr": {
			#					"$and": [
			#					{
			#						"$eq": ["$user_id","$$userId"]
			#					},
			#					{
			#						"$eq": ["$course_id","$$courseId"]
			#					},
			#					{
			#						"$eq": ["$course_id",course_id]
			#					}
			#					]
			#				}
			#			}
			#		}
			#		],
			#		"as": "user_videos"
			#	},
			#},
			#{
			#	"$lookup" : {
			#		"from": "seen_problems",
			#		"let": {
			#			"userId": "$user_id",
			#			"courseId": "$course_id"
			#		},
			#		"pipeline": [
			#		{
			#			"$match": {
			#				"$expr": {
			#					"$and": [
			#					{
			#						"$eq": ["$user_id","$$userId"]
			#					},
			#					{
			#						"$eq": ["$course_id","$$courseId"]
			#					},
			#					{
			#						"$eq": ["$course_id",course_id]
			#					},								
			#					]
			#				}
			#			}
			#		}
			#		],
			#		"as": "user_problems"
			#	},
			#},
			{
				"$project": {
					"_id" : 0,
					"username": "$users.username",
					"lower_username": { "$toLower": "$users.username" },
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
					#"different_problems": {"$toDouble" : { "$size": "$user_problems" }},
					#"different_videos": {"$toDouble" : { "$size": "$user_videos" }},
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

	def getSeenVideos(self, course_id):
		seen_videos = self.db.seen_videos.find({'course_id' : course_id}).sort([('time_day',1)])
		return [seen_video for seen_video in seen_videos]

	def getSeenProblems(self, course_id):
		seen_problems = self.db.seen_problems.find({'course_id' : course_id}).sort([('time_day',1)])
		return [seen_problem for seen_problem in seen_problems]

	def getSeenVideosAggrData(self):
		pipeline = [
			{
				"$group": {
					"_id": {
						"user_id": "$user_id",
						"course_id": "$course_id",
						#"time_day": "$time_day"
					},
					"user_id": {"$first" : "$user_id"},
					"course_id": {"$first" : "$course_id"},
					#"time_day": {"$first" : "$time_day"},
					"num_videos": { "$sum": 1 },
				}
			},
			{
				"$sort": {"num_videos" : 1},
			}

		]
		data_cursor = self.db.seen_videos.aggregate(pipeline)
		return [data for data in data_cursor]

	def getSeenProblemsAggrData(self):
		pipeline = [
			{
				"$group": {
					"_id": {
						"user_id": "$user_id",
						"course_id": "$course_id",
						#"time_day": "$time_day"
					},
					"user_id": {"$first" : "$user_id"},
					"course_id": {"$first" : "$course_id"},
					"diff_problems": { "$sum": 1 },
				},
			}
		]
		data_cursor = self.db.seen_problems.aggregate(pipeline)
		return [data for data in data_cursor]

	def getEventTrackAggrData(self):
		pipeline = [
			{
				"$group": {
					"_id": {
						"user_id": "$user_id",
						"course_id": "$course_id",
						#"time_day": "$time_day",
						"type_summary": "$type_summary"
					},
					"user_id": {"$first" : "$user_id"},
					"course_id": {"$first" : "$course_id"},
					#"time_day": "$time_day",
					"type_summary": {"$first" : "$type_summary"},				
					"num_problems": { "$sum": 1 },
				}
			},
			{
				"$sort": {
					"_id.user_id" : 1,
					"_id.type_summary" : 1,
					#"_id.time_day" : 1
				}
			},
			{"$project": {"_id" : 0,}},
		]
		data_cursor = self.db.event_track.aggregate(pipeline)
		return [data for data in data_cursor]

	def getFinalIndicatorsData(self, course_id):
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
					"lower_username": { "$toLower": "$users.username" },
					"user_id": 1,
					"time_day": 1,
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
				}
			},
			{
				"$sort": {
					"lower_username" : 1,
					"time_day" : 1,
				}
			},
			{
				"$project": {
					"lower_username": 0,
				}
			},
		]
		data_cursor = self.db.final_indicators.aggregate(pipeline, allowDiskUse = True)
		return [data for data in data_cursor]

	def updateConnectedDaysTime(self, course_id):
		indicators = self.getFinalIndicatorsData(course_id)
		last_user_id = -1
		for indicator in indicators:
			user_id = indicator['user_id']
			if user_id != last_user_id:
				connected_days = 0
				consecutive_inactivity_days = 0
				last_user_id = user_id
				print("Next User_id: ", user_id)
			if self.checkIndicatorActivity(indicator):
				connected_days += 1
				consecutive_inactivity_days = 0
			else:
				consecutive_inactivity_days += 1

			indicator['connected_days'] = connected_days
			indicator['consecutive_inactivity_days'] = consecutive_inactivity_days
			query = {'course_id' : course_id, 'user_id' : indicator['user_id'], 'time_day' : indicator['time_day']} 
			self.db.final_indicators.update_one(query, {'$set':indicator})

	def checkIndicatorActivity(self, indicator):
		if indicator['num_events'] == 0 and indicator['total_time'] == 0:
			return False
		else:
			return True

	def getUserLastSeenInfo(self, user_id, course_id):
		cursor = self.db.final_indicators.find({'user_id' : user_id, 'course_id' : course_id}).sort([('time_day',-1)]).limit(1)
		if cursor.count() == 0:
			return 0, 0
		else:
			indicator = cursor.next()
			return indicator['different_videos'], indicator['different_problems']


	def getUserLastActivityInfo(self, user_id, course_id):
		cursor = self.db.final_indicators.find({'user_id' : user_id, 'course_id' : course_id}).sort([('time_day',-1)]).limit(1)
		if cursor.count() == 0:
			return 0, 0
		else:
			indicator = cursor.next()
			return indicator['consecutive_inactivity_days'], indicator['connected_days']

	def importCollection(self, info, course_id, collection_name):
		query = {"course_id" : course_id}
		if self.db[collection_name].count_documents(query) > 0:
			print("WARNING: Collection", collection_name, "is not empty. The insertion will not take place")
		else:
			self.db[collection_name].insert(info)
		return

	def insertCourseInfo(self, course_name, course_id):
		query = {'course_id' : course_id}
		if (self.db.course_info.count_documents(query) == 0):
			self.db.course_info.insert_one({"course_name" : course_name, "course_id" : course_id})
		else:
			self.db.course_info.update_one(query, {'$set':{"course_name": course_name}})
		return 

	####################

	def getCurrentInactivityDays(self, course_id):
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
					#	"user_id": "$user_id",
					#	"consecutive_inactivity_days": {"$first" : "$consecutive_inactivity_days"},
					#},
					#"user_id": {"$first" : "$user_id"},
					#"course_id": {"$first" : "$course_id"},
					#"time_day": {"$first" : "$time_day"},
					#"num_videos": { "$sum": 1 },
				}
			},
			{
				"$project": {
					"_id": 0,
				}
			},

		]

		data_cursor = self.db.final_indicators.aggregate(pipeline, allowDiskUse = True)
		return [data for data in data_cursor]

	# close conection
	def closeConection(self):
		self.client.close()


# test
if __name__ == '__main__':

	edx = EdxMongoStore()
	
	BASE_PATH = "C:\\Users\\Juan\\Desktop\\edX-istaladores\\edx-mas_mongo\\data\\DatosAnonimizados\\"

	courses = [
		#{"course":"Quijote501x", "run":"3T2016", "path": "Quijote\\Quijote501x_2016_3T\\certificate\\CertificateQuijote501x3T2016.csv"},
		#{"course":"Quijote501x", "run":"3T2015", "path": "Quijote\\Quijote501x_2015_3T\\certificate\\CertificateQuijote501x3T2015.csv"},
		#{"course":"Quijote501x", "run":"1T2015", "path": "Quijote\\Quijote501x_2015_1T\\certificate\\CertificateQuijote501x1T2015.csv"},
		#{"course":"Renal701x", "run":"3T2016", "path": "Renal\\Renal701x_2016_3T\\certificate\\CertificateRenal701x3T2016.csv"},
		#{"course":"Renal701x", "run":"1T2016", "path": "Renal\\Renal701x_2016_1T\\certificate\\CertificateRenal701x1T2016.csv"},
		{"course":"Equidad801x", "run":"1T2016", "path": "Equidad801x_2016_1T\\certificate\\CertificateEquidad801x1T2016.csv"},
		#{"course":"Equidad801x", "run":"3T2016", "path": "Equidad\\Equidad801x_2016_3T\\certificate\\CertificateEquidad801x3T2016.csv"},
		#{"course": "Idealismo501x", "run": "1T2016", "path" : "Idealismo\\Idealismo501x_2016_1T\\certificate\\CertificateIdealismo501x1T2016.csv"}
	]

	for course in courses:
		edx.saveTypeFromCsv(BASE_PATH + course["path"], course["course"], course["run"])
