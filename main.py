from apscheduler.schedulers.blocking import BlockingScheduler

from core.cron.cron_statistik_pegawai import CronStatistikPegawai
from core.cron.cron_tanggungan import CronTanggungan

scheduler = BlockingScheduler()

# Interval Scheduling
scheduler.add_job(CronTanggungan().execute, 'interval', minutes=30)

# Cron Style scheduling
scheduler.add_job(CronStatistikPegawai().execute, 'cron', hour=0, minute=0)


def main():
    try:
        scheduler.start()
        print("Scheduler started")
    except (KeyboardInterrupt, SystemExit):
        print("Shutting down scheduler")


if __name__ == "__main__":
    main()
