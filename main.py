from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from core.config import LOGGER
from core.cron.cron_statistik_pegawai import CronStatistikPegawai
from core.cron.cron_tanggungan import CronTanggungan


def job_listener(event):
    if event.exception:
        LOGGER.error(f"❌ JOB GAGAL: {event.job_id} - {event.exception}")
    else:
        LOGGER.info(f"✅ JOB BERHASIL: {event.job_id}")


def execute_tanggungan():
    now = datetime.now()
    month = now.month
    CronTanggungan().execute(month)


def setup_scheduler():
    scheduler = BlockingScheduler()

    # Schedule setiap bulan tanggal 1
    scheduler.add_job(
        execute_tanggungan,
        'cron',
        day=1,
        hour=0,
        minute=0,
        id="cron_tanggungan",
        name="Cron Bulanan Tanggal 1",
        misfire_grace_time=3600,
    )

    # Cron Style scheduling
    scheduler.add_job(CronStatistikPegawai().execute, 'cron', hour=0, minute=0)

    return scheduler


def main():
    try:
        scheduler = setup_scheduler()
        scheduler.start()
        print("Scheduler started")
    except KeyboardInterrupt:
        LOGGER.info("\n⏹️  Menerima sinyal interrupt, menghentikan scheduler...")
    except SystemExit:
        print("Shutting down scheduler")
    except Exception as e:
        LOGGER.critical(f"💥 CRITICAL ERROR: {e}", exc_info=True)
    finally:
        LOGGER.info("=" * 60)
        LOGGER.info("🛑 APLIKASI SCHEDULER DIHENTIKAN")
        LOGGER.info(f"Waktu selesai: {datetime.now()}")
        LOGGER.info("=" * 60)


if __name__ == "__main__":
    main()
