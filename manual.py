#!/usr/bin/env python3
"""
Manual RUN cronjob
Script untuk menjalankan cronjob secara manual dengan pendekatan dinamis
"""

import argparse
import sys
from importlib import import_module

# Mapping cronjob names to module paths
CRONJOBS = {
    "statistik-pegawai": "core.cron.cron_statistik_pegawai.CronStatistikPegawai",
    "tanggungan": "core.cron.cron_tanggungan.CronTanggungan",
    # Tambahkan cronjob lain di sini
}


def load_cronjob_class(module_path):
    """
    Dynamically load cronjob class from module path
    """
    try:
        module_name, class_name = module_path.rsplit('.', 1)
        module = import_module(module_name)
        cron_class = getattr(module, class_name)
        return cron_class
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Gagal memuat cronjob class: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Manual RUN cronjob",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Contoh penggunaan:
  python {sys.argv[0]} statistik-pegawai
  python {sys.argv[0]} tanggungan -v
  python {sys.argv[0]} --list

Daftar cronjob yang tersedia:
{chr(10).join(f'  - {name}' for name in CRONJOBS.keys())}
        """
    )

    # Required argument
    parser.add_argument(
        'cronjob',
        type=str,
        nargs='?',  # Make it optional to support --list
        help="Nama cronjob yang akan dijalankan"
    )

    # Optional arguments
    parser.add_argument(
        '-l', '--list',
        action='store_true',
        help="Tampilkan daftar cronjob yang tersedia"
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Mode verbose - tampilkan informasi detail"
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Simulasi eksekusi tanpa benar-benar menjalankan"
    )

    args = parser.parse_args()

    # Handle --list option
    if args.list:
        print("Daftar cronjob yang tersedia:")
        for name in CRONJOBS.keys():
            print(f"  - {name}")
        return

    # Validate cronjob argument
    if not args.cronjob:
        parser.error("Argument 'cronjob' diperlukan (gunakan --list untuk melihat pilihan)")

    if args.cronjob not in CRONJOBS:
        parser.error(f"Cronjob '{args.cronjob}' tidak valid. Gunakan --list untuk melihat pilihan yang tersedia")

    # Execute cronjob
    if args.verbose:
        print(f"Memulai cronjob: {args.cronjob}")
        print(f"Module path: {CRONJOBS[args.cronjob]}")
        print(f"Dry run: {args.dry_run}")

    if args.dry_run:
        print(f"DRY RUN: Akan menjalankan {CRONJOBS[args.cronjob]}().execute()")
        return

    try:
        # Dynamically load and execute cronjob
        cron_class = load_cronjob_class(CRONJOBS[args.cronjob])
        cron_instance = cron_class()

        if args.verbose:
            print(f"Menjalankan execute() pada {cron_class.__name__}...")

        cron_instance.execute()

        if args.verbose:
            print(f"CronJob '{args.cronjob}' berhasil dijalankan!")

    except ImportError as e:
        print(f"ERROR: Gagal memuat module - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Gagal menjalankan cronjob - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
