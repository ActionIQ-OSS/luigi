import itertools
import mock
import time
from helpers import unittest
from nose.plugins.attrib import attr
import luigi.notifications
from luigi.scheduler import DISABLED, DONE, FAILED, PENDING, \
    UNKNOWN, RUNNING, BATCH_RUNNING, UPSTREAM_RUNNING, Scheduler

luigi.notifications.DEBUG = True
WORKER = 'myworker'

class SchedulerApiTest(unittest.TestCase):

    def setUp(self):
        super(SchedulerApiTest, self).setUp()
        conf = self.get_scheduler_config()
        self.sch = Scheduler(**conf)
        self.time = time.time

    def get_scheduler_config(self):
        return {
            'retry_delay': 100,
            'done_remove_delay': 1000,
            'disabled_remove_delay': 10000,
            'worker_disconnect_delay': 10,
            'disable_persist': 10,
            'disable_window': 10,
            'retry_count': 3,
            'disable_hard_timeout': 60 * 60,
            'stable_done_cooldown_secs': 0,
            'use_sql_state': True,
            'sql_target': 'sqlite:///:memory:'
        }

    def tearDown(self):
        super(SchedulerApiTest, self).tearDown()
        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_simple_dag(self):
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A',))
        self.sch.add_task(worker=WORKER, task_id='A')
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'A')
        self.sch.add_task(worker=WORKER, task_id='A', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], 'B')
        self.sch.add_task(worker=WORKER, task_id='B', status=DONE)
        self.assertEqual(self.sch.get_work(worker=WORKER)['task_id'], None)

    def test_dump_load_is_consistent(self):
        self.sch.add_task(worker=WORKER, task_id='B', deps=('A', 'E'), status=PENDING)
        self.sch.add_task(worker=WORKER, task_id='A', status=PENDING)
        self.sch.add_task(worker=WORKER, task_id='C', status=FAILED)
        self.sch.add_task(worker=WORKER, task_id='D', status=DISABLED)
        self.sch.add_task(worker=WORKER, task_id='E', status=DONE)
        old_state = set(self.sch._state.get_active_tasks())
        self.sch._state.dump()
        self.sch._state.load()
        new_state = set(self.sch._state.get_active_tasks())
        for t in old_state:
            t.failures = None # doesn't serialize right, repr just contains the mem address
        for t in new_state:
            t.failures = None # doesn't serialize right, repr just contains the mem address
        self.assertEqual(set(str(i) for i in old_state), set(str(i) for i in new_state))

    def test_persist_task_hits_db(self):
        pass

    def test_inactivate_task_hits_db(self):
        pass

    def test_get_task_with_setdefault_hits_db(self):
        pass

    def test_get_task_without_setdefault_skips_db(self):
        pass
