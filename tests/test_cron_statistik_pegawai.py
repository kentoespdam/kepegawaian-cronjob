from unittest import TestCase

from core.cron.cron_statistik_pegawai import CronStatistikPegawai


class TestCronStatistikPegawai(TestCase):
    def test_execute(self):
        CronStatistikPegawai().execute()
        self.assertEqual(True, True)
