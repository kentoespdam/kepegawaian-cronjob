#!/usr/bin/env python3
"""
Manual RUN cronjob
Script untuk menjalankan cronjob secara manual dengan pendekatan dinamis
"""

import argparse
import sys
from importlib import import_module
from datetime import datetime

# Mapping cronjob names to module paths dan parameter yang dibutuhkan
CRONJOBS = {
    "statistik-pegawai": {
        "module": "core.cron.cron_statistik_pegawai.CronStatistikPegawai",
        "params": []
    },
    "tanggungan": {
        "module": "core.cron.cron_tanggungan.CronTanggungan",
        "params": ["bulan"]
    },
    # Tambahkan cronjob lain di sini
    # "laporan-bulanan": {
    #     "module": "core.cron.cron_laporan.CronLaporan",
    #     "params": ["bulan", "tahun"]
    # },
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


def validate_bulan(value):
    """
    Validasi input bulan (1-12)
    """
    try:
        bulan = int(value)
        if bulan < 1 or bulan > 12:
            raise argparse.ArgumentTypeError("Bulan harus antara 1-12")
        return bulan
    except ValueError:
        raise argparse.ArgumentTypeError("Bulan harus berupa angka")


def validate_tahun(value):
    """
    Validasi input tahun
    """
    try:
        tahun = int(value)
        current_year = datetime.now().year
        if tahun < 2000 or tahun > current_year + 5:
            raise argparse.ArgumentTypeError(f"Tahun harus antara 2000-{current_year + 5}")
        return tahun
    except ValueError:
        raise argparse.ArgumentTypeError("Tahun harus berupa angka")


def main():
    parser = argparse.ArgumentParser(
        description="Manual RUN cronjob",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
            Contoh penggunaan:
              python {sys.argv[0]} statistik-pegawai
              python {sys.argv[0]} tanggungan --bulan 12
              python {sys.argv[0]} tanggungan -b 6 -v
              python {sys.argv[0]} --list
            
            Daftar cronjob yang tersedia:
            {chr(10).join(f'  - {name}: {info["params"]}' for name, info in CRONJOBS.items())}
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

    # Parameter khusus untuk berbagai cronjob
    parser.add_argument(
        '-b', '--bulan',
        type=validate_bulan,
        help="Bulan (1-12) - untuk cronjob yang membutuhkan parameter bulan"
    )

    parser.add_argument(
        '-t', '--tahun',
        type=validate_tahun,
        default=datetime.now().year,
        help="Tahun (default: tahun sekarang) - untuk cronjob yang membutuhkan parameter tahun"
    )

    args = parser.parse_args()

    # Handle --list option
    if args.list:
        print("Daftar cronjob yang tersedia:")
        for name, info in CRONJOBS.items():
            params_info = f" (parameter: {', '.join(info['params'])})" if info['params'] else ""
            print(f"  - {name}{params_info}")
        return

    # Validate cronjob argument
    if not args.cronjob:
        parser.error("Argument 'cronjob' diperlukan (gunakan --list untuk melihat pilihan)")

    if args.cronjob not in CRONJOBS:
        parser.error(f"Cronjob '{args.cronjob}' tidak valid. Gunakan --list untuk melihat pilihan yang tersedia")

    # Validasi parameter yang dibutuhkan
    cronjob_info = CRONJOBS[args.cronjob]
    required_params = cronjob_info["params"]

    missing_params = []
    for param in required_params:
        if param == "bulan" and not args.bulan:
            missing_params.append("--bulan")
        elif param == "tahun" and not args.tahun:
            missing_params.append("--tahun")

    if missing_params:
        parser.error(f"Cronjob '{args.cronjob}' membutuhkan parameter: {', '.join(missing_params)}")

    # Warning untuk parameter yang tidak diperlukan
    unused_params = []
    if args.bulan and "bulan" not in required_params:
        unused_params.append("--bulan")
    if args.tahun and "tahun" not in required_params:
        unused_params.append("--tahun")

    if unused_params and args.verbose:
        print(f"Peringatan: Parameter {', '.join(unused_params)} diabaikan untuk cronjob '{args.cronjob}'")

    # Execute cronjob
    if args.verbose:
        print(f"Memulai cronjob: {args.cronjob}")
        print(f"Module path: {cronjob_info['module']}")
        print(f"Dry run: {args.dry_run}")
        if args.bulan:
            print(f"Bulan: {args.bulan}")
        if args.tahun:
            print(f"Tahun: {args.tahun}")

    if args.dry_run:
        # Build parameter string for dry run
        params_str = ""
        if "bulan" in required_params and args.bulan:
            params_str += f"bulan={args.bulan}"
        if "tahun" in required_params and args.tahun:
            if params_str:
                params_str += ", "
            params_str += f"tahun={args.tahun}"

        if params_str:
            print(f"DRY RUN: Akan menjalankan {cronjob_info['module']}().execute({params_str})")
        else:
            print(f"DRY RUN: Akan menjalankan {cronjob_info['module']}().execute()")
        return

    try:
        # Dynamically load and execute cronjob
        cron_class = load_cronjob_class(cronjob_info['module'])
        cron_instance = cron_class()

        # Prepare execution parameters
        execute_kwargs = {}
        if "bulan" in required_params and args.bulan:
            execute_kwargs["bulan"] = args.bulan
        if "tahun" in required_params and args.tahun:
            execute_kwargs["tahun"] = args.tahun

        if args.verbose:
            if execute_kwargs:
                params_display = ", ".join(f"{k}={v}" for k, v in execute_kwargs.items())
                print(f"Menjalankan execute({params_display}) pada {cron_class.__name__}...")
            else:
                print(f"Menjalankan execute() pada {cron_class.__name__}...")

        # Eksekusi dengan parameter yang sesuai
        result = cron_instance.execute(**execute_kwargs)

        if args.verbose:
            print(f"CronJob '{args.cronjob}' berhasil dijalankan!")
            if result:
                print(f"Result: {result}")

    except ImportError as e:
        print(f"ERROR: Gagal memuat module - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Gagal menjalankan cronjob - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
