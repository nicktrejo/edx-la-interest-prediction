# -*- coding: utf-8 -*-
"""
Created on Thu May 14 19:00:00 2020
Version: 2.0
@author: nicktrejo
"""

import json  # JSON encoder and decoder
import re  # Regular expression operations


# Please refer to this documentation:
# https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html
# https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/sql_schema.html

class EdxEvent:
    """
    The EdXEvent object aims to extract info from a singular log edx event

    More info:
    https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/tracking_logs.html
    :param raw_event: the raw singular event (line) as it is in the log file
    :type raw_event: str
    """

    def __init__(self, raw_event):
        """
        Constructor method
        :param raw_event: the raw singular event (line) as it is in the log file
        :type raw_event: str
        """
        if isinstance(raw_event, dict):
            self.event = raw_event
        else:
            self.event = json.loads(raw_event)

        try:
            self.element = json.loads(self.event['event'])
        except (TypeError, ValueError):
            try:
                self.element = self.event['event']
            except NameError:
                self.element = dict()

        self.user_id = self.get_user_id()
        self.course_id = self.get_course_id()
        try:
            self.org_course_run = re.search(':(.*)', self.course_id).group(1)
        except AttributeError:
            self.org_course_run = None
        self.time = self.get_time()
        self.event_type = self.get_event_type()


    def get_user_id(self) -> str:
        """
        Get user id from the event

        :return: user_id or None
        :rtype: str
        """
        result = None
        try:
            result = self.event['context']['user_id']
        except KeyError:
            pass
        if not result:
            try:
                result = self.element['user_id']
            except (TypeError, KeyError):
                # No user_id found, will return None
                result = None
        return result


    def get_course_id(self) -> str:
        """
        Get course id from the event

        :return: course_id or None
        :rtype: str
        """
        result = self.event['context']['course_id']
        if not result:
            try:
                result = self.element['course_id']
            except (TypeError, KeyError):
                # No course_id found, will return None
                result = None
        return result


    def get_time(self) -> str:
        """
        Get time from the event

        :return: time
        :rtype: str
        """
        result = self.event['time']
        return result


    def get_event_type(self) -> str:
        """
        Get event type from the event

        :return: event_type
        :rtype: str
        """
        result = self.event['event_type']
        return result


    def get_video_id(self) -> str:
        """
        Get video id from the event

        :return: video_id or None
        :rtype: str
        """
        try:
            result = self.element['id']
        except (TypeError, KeyError):
            # No video_id found, will return None
            result = None
        return result


    def get_video_code(self) -> str:
        """
        Get video code from the event

        :return: video_code or None
        :rtype: str
        """
        try:
            result = self.element['code']
        except (TypeError, KeyError):
            # No video_id found, will return None
            result = None
        return result


    def get_module_id(self) -> str:
        """
        Get (problem's) module id from the event

        :return: module_id or None
        :rtype: str
        """
        result = None
        if isinstance(self.element, dict):
            try:
                result = self.element['problem_id']
            except KeyError:
                try:
                    result = self.element['module_id']
                except KeyError:
                    try:
                        result = self.element['problem']
                    except KeyError:
                        result = None
        else:  # manual build of module_id if self.event['event'] is list / str
            if isinstance(self.element, list):
                module_hash_aux = self.element[0].split('_')
            elif isinstance(self.element, str):
                module_hash_aux = self.element.split('_')
            else:
                module_hash_aux = []
            if module_hash_aux[0] == 'input':
                module_hash = module_hash_aux[1]
                if len(module_hash) == 32:
                    result = 'block-v1:' + self.org_course_run + \
                                '+type@problem+block@' + module_hash
        if not result:  # if module_id is still None or ''
            # https://edx.readthedocs.io/projects/devdata/en/stable/internal_data_formats/sql_schema.html#module-id
            # example:
            # block-v1:edX+DemoX+Demo_2014+type@problem+block@303034da25524878a2e66fb57c91cf85
            module_id_pattern = re.escape('block-v1:' + self.course_id
                                          + '+type@problem+block@')
            module_id_pattern = module_id_pattern + '.{32}'
            module_id_aux = re.search(module_id_pattern, self.event['referer'])
            if module_id_aux:
                result = module_id_aux.group()
        return result


    def get_block_id(self) -> str:
        """
        Get block id (module name or hash code) from the event

        :return: block_id or None
        :rtype: str
        """
        module_id = self.get_module_id()
        result = None
        if isinstance(module_id, str) and len(module_id) >= 32:
            result = module_id[-32:]
        if not result:  # if module_id is still None or ''
            result = self.get_video_id()
        return result


    def get_grade(self) -> float:
        """
        Get (problem's) grade from the event

        :return: grade or None
        :rtype: int, float
        """
        try:
            result = self.element['grade']
        except (TypeError, KeyError):
            # No grade found, will return None
            result = None
        return result


    def get_max_grade(self) -> float:
        """
        Get (problem's) max_grade from the event

        :return: max_grade or None
        :rtype: int, float
        """
        try:
            result = self.element['max_grade']
        except (TypeError, KeyError):
            # No max_grade found, will return None
            result = None
        return result


    def get_attempts(self) -> float:
        """
        Get (problem's) attempts from the event

        :return: attempts or None
        :rtype: int, float
        """
        try:
            result = self.element['attempts']
        except (TypeError, KeyError):
            # No attempts found, will return None
            result = None
        return result


    def get_enrollment_mode(self) -> str:
        """
        Get enrollment_mode (aka mode) from the event

        :return: enrollment_mode or None
        :rtype: str
        """
        # enrollment_mode = { 'audit', 'honor', 'verified', blank }
        try:
            result = self.element['mode']
        except (TypeError, KeyError):
            try:
                result = self.element['enrollment_mode']
            except (TypeError, KeyError):
                # No mode found, will return None
                result = None
        return result


    def __str__(self):
        """
        String representation of the edxevent

        :return: string representation
        :rtype: str
        """
        return str(self.event)


if __name__ == '__main__':
    # It will test EdxEvent class
    def test_func(data):
        """
        Test all attributes and methods for EdxEvent class printing results

        :return: None
        """
        print('# TEST')
        _ee = EdxEvent(data)
        my_dict = {
            # 'event': _ee.event,
            'element': _ee.element,
            'user_id': _ee.user_id,
            'course_id': _ee.course_id,
            'org_course_run': _ee.org_course_run,
            'time': _ee.time,
            'event_type': _ee.event_type,
            'get_user_id': _ee.get_user_id(),
            'get_course_id': _ee.get_course_id(),
            'get_event_type': _ee.get_event_type(),
            'get_video_id': _ee.get_video_id(),
            'get_video_code': _ee.get_video_code(),
            'get_module_id': _ee.get_module_id(),
            'get_block_id': _ee.get_block_id(),
            'get_grade': _ee.get_grade(),
            'get_max_grade': _ee.get_max_grade(),
            'get_attempts': _ee.get_attempts(),
            'get_enrollment_mode': _ee.get_enrollment_mode(),
        }
        print('\tRAW EVENT: {0}'.format(_ee))
        print('\tPROCESSED EVENT: {0}'.format(my_dict))

    test_1 = (r'{"username": "24905909", "event_source": "browser", "name": "play_video", "accept_l'
              r'anguage": "es-US,es-419;q=0.9,es;q=0.8,en;q=0.7", "time": "2019-08-17T14:21:55.9391'
              r'97+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64 AppleWebKit/537.36 (K'
              r'HTML, like Gecko Chrome/74.0.3729.131 Safari/537.36", "page": "https://courses.edx.'
              r'org/courses/course-v1:UAMx+WebApp+1T2019a/courseware/2062d8c9518447c7b1c6749f85df98'
              r'df/7a68f4e1e27f4792a4db06b79f145714/?child=first", "host": "courses.edx.org", "sess'
              r'ion": "ae1631db9a779d5efe8b5e96768c9da8", "referer": "https://courses.edx.org/cours'
              r'es/course-v1:UAMx+WebApp+1T2019a/courseware/2062d8c9518447c7b1c6749f85df98df/7a68f4'
              r'e1e27f4792a4db06b79f145714/?child=first", "context": {"user_id": 24905909, "org_id"'
              r': "UAMx", "course_id": "course-v1:UAMx+WebApp+1T2019a", "path": "/event"}, "ip": "c'
              r'f1a0ad6adfbb160acac18c5b84f7c1e", "event": "{\"duration\": 86.37, \"code\": \"hls\"'
              r', \"id\": \"f1890204d70d4ca3a807f0007bd7f598\", \"currentTime\": 61}", "event_type"'
              r': "play_video"}')

    test_2 = (r'{"username": "58798", "event_source": "browser", "name": "problem_show", "accept_la'
              r'nguage": "en-US,en;q=0.9", "time": "2019-06-19T14:04:21.430535+00:00", "agent": "Mo'
              r'zilla/5.0 (Windows NT 10.0; Win64; x64 AppleWebKit/537.36 (KHTML, like Gecko Chrome'
              r'/74.0.3729.169 Safari/537.36", "page": "https://courses.edx.org/courses/course-v1:U'
              r'AMx+WebApp+1T2019a/courseware/8af1aaa9e32541d0857dc48674600ba9/ee51e121eb2f43a09ca8'
              r'c26b4aa90220/5?activate_block_id=block-v1%3AUAMx%2BWebApp%2B1T2019a%2Btype%40vertic'
              r'al%2Bblock%407476d182ff7641e19a6f7d1204c17d5e", "host": "courses.edx.org", "session'
              r'": "7201b6ffc7ae1bdf4b30bcd0a5dac576", "referer": "https://courses.edx.org/courses/'
              r'course-v1:UAMx+WebApp+1T2019a/courseware/8af1aaa9e32541d0857dc48674600ba9/ee51e121e'
              r'b2f43a09ca8c26b4aa90220/5?activate_block_id=block-v1%3AUAMx%2BWebApp%2B1T2019a%2Bty'
              r'pe%40vertical%2Bblock%407476d182ff7641e19a6f7d1204c17d5e", "context": {"user_id": 5'
              r'8798, "org_id": "UAMx", "course_id": "course-v1:UAMx+WebApp+1T2019a", "path": "/eve'
              r'nt"}, "ip": "9375af7115ce5751f81d6a1e3be13694", "event": "{\"problem\": \"block-v1:'
              r'UAMx+WebApp+1T2019a+type@problem+block@d94fe7033f884651a21b77131c826d0e\"}", "event'
              r'_type": "problem_show"}')

    test_3 = (r'{"username": "23734780", "event_source": "server", "name": "edx.course.enrollment.a'
              r'ctivated", "accept_language": "es-US,es-VE;q=0.9,es-419;q=0.8,es;q=0.7", "time": "2'
              r'019-05-22T21:37:14.423975+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64'
              r' AppleWebKit/537.36 (KHTML, like Gecko Chrome/74.0.3729.157 Safari/537.36", "page":'
              r' null, "host": "courses.edx.org", "session": "b8145f0af9375290b1f40a91e717ba95", "r'
              r'eferer": "https://courses.edx.org/account/finish_auth?course_id=course-v1%3AUAMx%2B'
              r'WebApp%2B1T2019a&enrollment_action=enroll&email_opt_in=false&next=%2Fdashboard", "c'
              r'ontext": {"user_id": 23734780, "org_id": "UAMx", "course_id": "course-v1:UAMx+WebAp'
              r'p+1T2019a", "path": "/api/commerce/v0/baskets/"}, "ip": "e2341632ef7b6989c6a766b120'
              r'f44cc6", "event": {"course_id": "course-v1:UAMx+WebApp+1T2019a", "user_id": 2373478'
              r'0, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}')

    # tests = [test_1, test_2, test_3]

    # for test in tests:
    #     pass
    #     # test_func(test)

    # ASSERTIONS for TEST_1
    test_1_ee = EdxEvent(test_1)
    assert test_1_ee.element == {'duration': 86.37, 'code': 'hls',
                                        'id': 'f1890204d70d4ca3a807f0007bd7f598', 'currentTime': 61}
    assert test_1_ee.user_id == 24905909
    assert test_1_ee.course_id == 'course-v1:UAMx+WebApp+1T2019a'
    assert test_1_ee.org_course_run == 'UAMx+WebApp+1T2019a'
    assert test_1_ee.time == '2019-08-17T14:21:55.939197+00:00'
    assert test_1_ee.event_type == 'play_video'
    assert test_1_ee.get_user_id() == 24905909
    assert test_1_ee.get_course_id() == 'course-v1:UAMx+WebApp+1T2019a'
    assert test_1_ee.get_event_type() == 'play_video'
    assert test_1_ee.get_video_id() == 'f1890204d70d4ca3a807f0007bd7f598'
    assert test_1_ee.get_video_code() == 'hls'
    assert test_1_ee.get_module_id() is None
    assert test_1_ee.get_block_id() == 'f1890204d70d4ca3a807f0007bd7f598'
    assert test_1_ee.get_grade() is None
    assert test_1_ee.get_max_grade() is None
    assert test_1_ee.get_attempts() is None
    assert test_1_ee.get_enrollment_mode() is None

    test_2_ee = EdxEvent(test_2)
    assert test_2_ee.element == {'problem': 
        'block-v1:UAMx+WebApp+1T2019a+type@problem+block@d94fe7033f884651a21b77131c826d0e'}
    assert test_2_ee.user_id == 58798
    assert test_2_ee.course_id == 'course-v1:UAMx+WebApp+1T2019a'
    assert test_2_ee.org_course_run == 'UAMx+WebApp+1T2019a'
    assert test_2_ee.time == '2019-06-19T14:04:21.430535+00:00'
    assert test_2_ee.event_type == 'problem_show'
    assert test_2_ee.get_user_id() == 58798
    assert test_2_ee.get_course_id() == 'course-v1:UAMx+WebApp+1T2019a'
    assert test_2_ee.get_event_type() == 'problem_show'
    assert test_2_ee.get_video_id() is None
    assert test_2_ee.get_video_code() is None
    assert test_2_ee.get_module_id() == 'block-v1:UAMx+WebApp+1T2019a+type@problem+block@d94fe7033f884651a21b77131c826d0e'
    assert test_2_ee.get_block_id() == 'd94fe7033f884651a21b77131c826d0e'
    assert test_2_ee.get_grade() is None
    assert test_2_ee.get_max_grade() is None
    assert test_2_ee.get_attempts() is None
    assert test_2_ee.get_enrollment_mode() is None

    test_3_ee = EdxEvent(test_3)
    assert test_3_ee.element == {'course_id': 'course-v1:UAMx+WebApp+1T2019a',
                                        'user_id': 23734780, 'mode': 'audit'}
    assert test_3_ee.user_id == 23734780
    assert test_3_ee.course_id == 'course-v1:UAMx+WebApp+1T2019a'
    assert test_3_ee.org_course_run == 'UAMx+WebApp+1T2019a'
    assert test_3_ee.time == '2019-05-22T21:37:14.423975+00:00'
    assert test_3_ee.event_type == 'edx.course.enrollment.activated'
    assert test_3_ee.get_user_id() == 23734780
    assert test_3_ee.get_course_id() == 'course-v1:UAMx+WebApp+1T2019a'
    assert test_3_ee.get_event_type() == 'edx.course.enrollment.activated'
    assert test_3_ee.get_video_id() is None
    assert test_3_ee.get_video_code() is None
    assert test_3_ee.get_module_id() is None
    assert test_3_ee.get_block_id() is None
    assert test_3_ee.get_grade() is None
    assert test_3_ee.get_max_grade() is None
    assert test_3_ee.get_attempts() is None
    assert test_3_ee.get_enrollment_mode() == 'audit'

    print('Assertions passed OK')
