#!/usr/bin/env python3
"""
Wrapper script to submit ParqConverter.py jobs to qsub for a date range.
Submits one job per date.
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


def parse_date(date_str):
    """Parse date string in YYYYMMDD format."""
    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYYMMDD (e.g., 20250928)")


def generate_date_range(start_date_str, end_date_str):
    """
    Generate list of dates between start and end (inclusive).

    Args:
        start_date_str: Start date in YYYYMMDD format
        end_date_str: End date in YYYYMMDD format

    Returns:
        List of date strings in YYYYMMDD format
    """
    start = parse_date(start_date_str)
    end = parse_date(end_date_str)

    if start > end:
        raise ValueError(f"Start date {start_date_str} is after end date {end_date_str}")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return dates


def submit_qsub_job(config, table, date, log_dir, verbose, hostname=None, hold_jid=None):
    """
    Submit a single ParqConverter job to qsub.

    Args:
        config: Path to config file
        table: Table name
        date: Date string (YYYYMMDD)
        log_dir: Directory for log files
        verbose: Whether to use verbose mode
        hostname: Optional hostname to run on
        hold_jid: Optional job ID to wait for (for job dependencies)

    Returns:
        Job ID of submitted job
    """
    # Create log file path for this specific date
    log_file = Path(log_dir) / f"{date}_parqconverter.log"

    # Build the ParqConverter command
    # Hardcoded uv path for qsub environment
    uv_path = "/home/cds.jobs/.local/bin/uv"
    parqconverter_cmd = [
        uv_path,
        "run",
        "python",
        "/q/cw/common/user/sam/cw-parq-converter/src/parquet_converter/ParqConverter.py",
        "-c",
        config,
        "-t",
        table,
        "-d",
        date,
    ]

    if verbose:
        parqconverter_cmd.append("-v")

    # Build qsub command
    qsub_cmd = [
        "qsub",
        "-cwd",  # Run job in current working directory
        "-o",
        str(log_file),
        "-e",
        str(log_file),
        "-b",
        "y",  # Binary mode (command instead of script file)
    ]

    # Add optional hostname
    if hostname:
        qsub_cmd.extend(["-l", f"hostname={hostname}"])

    # Add job dependency if specified
    if hold_jid:
        qsub_cmd.extend(["-hold_jid", hold_jid])

    # Add the command to execute
    qsub_cmd.extend(parqconverter_cmd)

    # Submit the job
    print(f"Submitting job for {table} on {date}...")

    try:
        result = subprocess.run(qsub_cmd, capture_output=True, text=True, check=True)
        # Parse job ID from qsub output (format: "Your job 12345 ...")
        job_id = result.stdout.split()[2]
        print(f"  Job submitted: {job_id}")
        return job_id
    except subprocess.CalledProcessError as e:
        print(f"  Error submitting job: {e.stderr}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Submit ParqConverter jobs to qsub for a date range (one job per date)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - submit jobs for a date range
  %(prog)s -c config.yml -t my_table -s 20250901 -e 20250905 -l /path/to/logs/

  # With verbose logging
  %(prog)s -c config.yml -t my_table -s 20250901 -e 20250905 -l /path/to/logs/ -v

  # Specify hostname
  %(prog)s -c config.yml -t my_table -s 20250901 -e 20250905 -l /path/to/logs/ --hostname ip-10-211-78-82.ec2.internal

  # Sequential execution (each job waits for previous to finish)
  %(prog)s -c config.yml -t my_table -s 20250901 -e 20250905 -l /path/to/logs/ --sequential
        """,
    )

    parser.add_argument("-c", "--config", required=True, help="Path to the config file")
    parser.add_argument("-t", "--table", required=True, help="Name of the table to convert")
    parser.add_argument("-s", "--start", required=True, help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", required=True, help="End date (YYYYMMDD)")
    parser.add_argument(
        "-l",
        "--logs",
        required=True,
        help="Directory for log files (e.g., /path/to/logs/ creates {date}_parqconverter.log for each date)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose logging (debug)")
    parser.add_argument("--hostname", help="Specific hostname to run jobs on (optional)")
    parser.add_argument(
        "--sequential", action="store_true", help="Run jobs sequentially (each waits for previous to finish)"
    )

    args = parser.parse_args()

    # Validate config file exists
    if not Path(args.config).exists():
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    # Generate date range
    try:
        dates = generate_date_range(args.start, args.end)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Create log directory
    log_dir = Path(args.logs)
    log_dir.mkdir(parents=True, exist_ok=True)

    # Print summary
    print(f"Submitting {len(dates)} jobs for table '{args.table}' from {args.start} to {args.end}")
    print(f"Config: {args.config}")
    print(f"Log directory: {log_dir}")
    if args.sequential:
        print("Mode: Sequential (jobs run one after another)")
    else:
        print("Mode: Parallel (all jobs run simultaneously)")
    print("-" * 60)

    # Submit jobs
    job_ids = []
    previous_job_id = None

    for date in dates:
        hold_jid = previous_job_id if args.sequential else None
        job_id = submit_qsub_job(
            config=args.config,
            table=args.table,
            date=date,
            log_dir=log_dir,
            verbose=args.verbose,
            hostname=args.hostname,
            hold_jid=hold_jid,
        )

        if job_id:
            job_ids.append(job_id)
            if args.sequential:
                previous_job_id = job_id
        else:
            print(f"Warning: Failed to submit job for {date}", file=sys.stderr)

    print("-" * 60)
    print(f"Successfully submitted {len(job_ids)} out of {len(dates)} jobs")

    if job_ids:
        print(f"Job IDs: {', '.join(job_ids)}")
        print(f"\nTo monitor jobs: qstat -u $USER")
        print(f"To view logs: ls -lh {log_dir}")
        print(f"To tail a specific log: tail -f {log_dir}/<date>_parqconverter.log")


if __name__ == "__main__":
    main()
