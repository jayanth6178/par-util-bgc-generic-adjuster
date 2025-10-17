import argparse
import atexit
import logging
import resource
import time
from datetime import timedelta

from parquet_converter.administrators.base_administrator import Administrator

"""
TODO: ordered most important to least important:
    - Add support for zip files
    - Actually generate a manifest if missing (see manifest.py)
    - Implement filters (see csv_reader.py)
    - Ensure all other config features are added and supported
"""


def _parse_search_params(value):
    """Parse comma-separated key=value pairs into a dictionary."""
    if not value:
        return {}

    result = {}
    try:
        pairs = value.split(",")
        for pair in pairs:
            if "=" not in pair:
                raise argparse.ArgumentTypeError(f"Invalid format in '{pair}'. Expected 'key=value'")
            key, val = pair.split("=", 1)
            key = key.strip()
            val = val.strip()
            if not key:
                raise argparse.ArgumentTypeError(f"Empty key in '{pair}'")
            result[key] = val
        return result
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid parameter format: {e}")


def _arguments():
    parser = argparse.ArgumentParser(description="Parquet Converter")
    parser.add_argument("-c", "--config", type=str, required=True, help="Path to the config file")
    parser.add_argument("-t", "--table", type=str, required=True, help="Name of the table to convert")
    parser.add_argument("-d", type=str, required=True, help="The date/delta of the conversion")
    parser.add_argument("-v", "--verbose", action="store_true", help="Turn on verbose logging (debug)")
    parser.add_argument(
        "-p",
        "--search-params",
        type=_parse_search_params,
        default={},
        help="Additional parameters as comma-separated key=value pairs (e.g., 'thing1=value,another_thing=another_value')",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Override output path. Can be either a base directory or a full template path with variables like {table}, {YYYYMMDD}, etc.",
    )
    return parser.parse_args()


def log_on_exit(logger, start_time):
    elapsed = time.perf_counter() - start_time
    maxm_mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    logger.info(f"Total time taken: {timedelta(seconds=elapsed)}")
    logger.info(f"Max memory usage: {maxm_mem_mb:.2f} MB")


def main():
    args = _arguments()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)
    start_time = time.perf_counter()
    atexit.register(log_on_exit, logger, start_time)

    administrator = Administrator(logger, args.config, args.table, output_override=args.output)
    administrator.process(args.d, search_params=args.search_params)


if __name__ == "__main__":
    main()
