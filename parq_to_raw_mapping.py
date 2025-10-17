import os
import re
from parquet_converter.utils.config import Config
from parquet_converter.utils.finder import FileFinder


def parq_to_raw_mapping(parq_file, table, config_path, search_vars=None):
    """
    Maps a parquet file back to the raw file(s) that produced it.
    
    Args:
        parq_file (str): Path to the parquet file
        table (str): Table name (e.g., 'sales_data')
        config_path (str): Path to the YAML config file
        search_vars (dict, optional): Additional search parameters to pass to FileFinder
    
    Returns:
        list: List of RawFileInfo objects representing the raw files
    """
    
    # Load config
    config = Config.from_yaml(config_path)
    table_config = config.get_table_config(table)
    
    # Get the output template (how parquet files are named)
    output_template = table_config.output_file_template or config.output.file_template
    basedir = config.output.basedir
    
    # Extract variables from the parquet filename
    extract_vars = _parse_parquet_filename(
        parq_file=parq_file,
        output_template=output_template,
        basedir=basedir
    )
    
    # Merge with any additional search vars provided by user
    if search_vars:
        extract_vars.update(search_vars)
    
    # Ensure 'table' is in extract_vars
    extract_vars['table'] = table
    
    # Separate date/time components from other search parameters
    # FileFinder uses date/time components for its own formatting, so they shouldn't be in search_params
    # Everything else (including seq_num, table, etc.) should be passed as search_params
    reserved_keys = {
        'YYYY', 'MM', 'DD', 'YYYYMMDD', 'YYYYMM', 'YYMMDD', 'YYMM', 'YY',
        'hh', 'mm', 'ss', 'ms', 'us', 'delta'
    }
    
    # search_params contains all non-date variables (including seq_num, table, etc.)
    search_params = {k: v for k, v in extract_vars.items() if k not in reserved_keys}
    
    # Determine the date range for searching
    date_str = extract_vars.get('YYYYMMDD')
    if not date_str:
        raise ValueError(f"Could not extract date from parquet file: {parq_file}")
    
    # Find raw files for each raw_file configuration
    all_raw_files = []
    
    # Get the global input configuration for file_type
    input_config = config.input if hasattr(config, 'input') else {}
    global_file_type = input_config.get('file_type', 'date') if isinstance(input_config, dict) else getattr(input_config, 'file_type', 'date')
    
    for raw_file_template in table_config.raw_files:
        # raw_file_template is just a string path with placeholders
        # Create FileFinder with the raw file template
        finder = FileFinder(
            file_template=raw_file_template,
            search_params=search_params,  # Includes seq_num, table, and any other non-date params
            file_type=global_file_type
        )
        
        # Search for raw files on this specific date
        raw_files = finder.find_range(after=date_str, before=date_str)
        all_raw_files.extend(raw_files)
    
    return all_raw_files


def _parse_parquet_filename(parq_file, output_template, basedir):
    """
    Extracts variables from a parquet filename using the output template.
    
    This is the reverse of formatting: given a template and a filename,
    extract the values that were used to create that filename.
    
    Handles duplicate placeholders (e.g., {YYYYMMDD} appearing twice) by creating
    unique capture groups and validating they match.
    
    Args:
        parq_file (str): Full path to parquet file
        output_template (str): Template pattern 
        basedir (str): Base directory for parquet files
    
    Returns:
        dict: Extracted variables
    """
    
    relative_path = os.path.relpath(parq_file, basedir)
    
    # Define patterns for each placeholder type
    placeholder_patterns = {
        'YYYYMMDD': r'\d{8}',
        'YYYYMM': r'\d{6}',
        'YYMMDD': r'\d{6}',
        'YYMM': r'\d{4}',
        'YYYY': r'\d{4}',
        'MM': r'\d{2}',
        'DD': r'\d{2}',
        'YY': r'\d{2}',
        'hh': r'\d{2}',
        'mm': r'\d{2}',
        'ss': r'\d{2}',
        'ms': r'\d{3}',
        'us': r'\d{6}',
        'delta': r'\d+',
        'table': r'[A-Za-z0-9_]+',
        'seq_num': r'\d+',
    }

    regex_pattern = re.escape(output_template)
    # Result: '\\{YYYYMMDD\\}\\.\\{seq_num:3\\}\\.\\{table\\}\\.parq'

    # Track placeholder occurrences to handle duplicates
    placeholder_counts = {}
    placeholder_groups = {}  # Maps group_name to placeholder_name
    # Replace the other standard placeholders with capture groups
    # Process in order of specificity (longest first to avoid partial matches)
    for placeholder in sorted(placeholder_patterns.keys(), key=len, reverse=True):
        escaped_placeholder = re.escape('{' + placeholder + '}')
        
        # Count occurrences of this placeholder
        count = 0
        while escaped_placeholder in regex_pattern:
            # Create unique group name for each occurrence
            group_name = f"{placeholder}_{count}" if count > 0 else placeholder
            placeholder_groups[group_name] = placeholder
            
            # Replace first occurrence with named capture group
            regex_pattern = regex_pattern.replace(
                escaped_placeholder,
                f"(?P<{group_name}>{placeholder_patterns[placeholder]})",
                1  # Replace only first occurrence
            )
            count += 1
        
        if count > 0:
            placeholder_counts[placeholder] = count
    
    # Match the relative path against the pattern
    match = re.match(regex_pattern, relative_path)
    
    if not match:
        raise ValueError(
            f"Parquet file path '{relative_path}' does not match template '{output_template}'\n"
            f"Full path: {parq_file}\n"
            f"Basedir: {basedir}"
        )
    
    # Extract all matched groups
    all_groups = match.groupdict()
    
    # Consolidate duplicate placeholders and validate they match
    extract_vars = {}
    for group_name, value in all_groups.items():
        # Get the original placeholder name
        placeholder = placeholder_groups[group_name]
        
        if placeholder in extract_vars:
            # Duplicate placeholder - verify values match
            if extract_vars[placeholder] != value:
                raise ValueError(
                    f"Duplicate placeholder {{{placeholder}}} has mismatched values: "
                    f"'{extract_vars[placeholder]}' vs '{value}' in path '{relative_path}'"
                )
        else:
            extract_vars[placeholder] = value
    
    # Fill in derived date formats (similar to FileFinder._extract_d_formater)
    _derive_date_components(extract_vars)
    
    return extract_vars


def _derive_date_components(extract_vars):
    """
    Derives missing date components from composite formats.
    
    Modifies extract_vars in place to add missing date components.
    For example, if YYYYMMDD exists, derives YYYY, MM, DD, etc.
    
    Args:
        extract_vars (dict): Dictionary of extracted variables (modified in place)
    """
    
    # Extract individual components from composite formats
    if 'YYYYMMDD' in extract_vars and extract_vars['YYYYMMDD']:
        yyyymmdd = extract_vars['YYYYMMDD']
        extract_vars.setdefault('YYYY', yyyymmdd[:4])
        extract_vars.setdefault('MM', yyyymmdd[4:6])
        extract_vars.setdefault('DD', yyyymmdd[6:8])
    
    if 'YYYYMM' in extract_vars and extract_vars['YYYYMM']:
        yyyymm = extract_vars['YYYYMM']
        extract_vars.setdefault('YYYY', yyyymm[:4])
        extract_vars.setdefault('MM', yyyymm[4:6])
    
    if 'YYMMDD' in extract_vars and extract_vars['YYMMDD']:
        yymmdd = extract_vars['YYMMDD']
        extract_vars.setdefault('YY', yymmdd[:2])
        extract_vars.setdefault('MM', yymmdd[2:4])
        extract_vars.setdefault('DD', yymmdd[4:6])
    
    if 'YYMM' in extract_vars and extract_vars['YYMM']:
        yymm = extract_vars['YYMM']
        extract_vars.setdefault('YY', yymm[:2])
        extract_vars.setdefault('MM', yymm[2:4])
    
    # Convert between YY and YYYY
    if 'YY' in extract_vars and extract_vars['YY'] and 'YYYY' not in extract_vars:
        import datetime as dt
        extract_vars['YYYY'] = str(dt.datetime.strptime(extract_vars['YY'], '%y').year)
    
    if 'YYYY' in extract_vars and extract_vars['YYYY'] and 'YY' not in extract_vars:
        extract_vars['YY'] = extract_vars['YYYY'][2:4]
    
    # Build composite formats from individual components
    if all(k in extract_vars for k in ('YYYY', 'MM', 'DD')):
        extract_vars.setdefault('YYYYMMDD', 
                                extract_vars['YYYY'] + extract_vars['MM'] + extract_vars['DD'])
    
    if all(k in extract_vars for k in ('YYYY', 'MM')):
        extract_vars.setdefault('YYYYMM', extract_vars['YYYY'] + extract_vars['MM'])
    
    if all(k in extract_vars for k in ('YY', 'MM', 'DD')):
        extract_vars.setdefault('YYMMDD', 
                                extract_vars['YY'] + extract_vars['MM'] + extract_vars['DD'])
    
    if all(k in extract_vars for k in ('YY', 'MM')):
        extract_vars.setdefault('YYMM', extract_vars['YY'] + extract_vars['MM'])


# Command-line interface
if __name__ == '__main__':
    import argparse
    import json
    
    parser = argparse.ArgumentParser(
        description='Map a parquet file back to the raw files that produced it.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python parq_to_raw_mapping.py /data/parquet/20241015.sales_data_2024_10_15.parq sales_data config.yaml
  
  # With sequence numbers
  python parq_to_raw_mapping.py /data/parquet/20251015.001.KeyDev.parq KeyDev config.yaml
  
  # With additional search variables
  python parq_to_raw_mapping.py /data/parquet/20241015.sales_data_2024_10_15.parq sales_data config.yaml --search-vars '{"region": "us-east"}'
  
  # Output as JSON
  python parq_to_raw_mapping.py /data/parquet/20241015.sales_data_2024_10_15.parq sales_data config.yaml --json
        """
    )
    
    parser.add_argument(
        'parq_file',
        help='Path to the parquet file'
    )
    
    parser.add_argument(
        'table',
        help='Table name (e.g., sales_data)'
    )
    
    parser.add_argument(
        'config_path',
        help='Path to the YAML config file'
    )
    
    parser.add_argument(
        '--search-vars',
        type=str,
        help='Additional search variables as JSON string (e.g., \'{"region": "us-east"}\')'
    )
    
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show detailed information about each raw file'
    )
    
    args = parser.parse_args()
    
    # Parse search_vars if provided
    search_vars = None
    if args.search_vars:
        try:
            search_vars = json.loads(args.search_vars)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON in --search-vars: {e}")
            exit(1)
    
    # Find raw files
    try:
        raw_files = parq_to_raw_mapping(
            parq_file=args.parq_file,
            table=args.table,
            config_path=args.config_path,
            search_vars=search_vars
        )
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
    
    # Output results
    if args.json:
        # JSON output format
        output = {
            'parquet_file': args.parq_file,
            'table': args.table,
            'raw_files_count': len(raw_files),
            'raw_files': []
        }
        
        for rf in raw_files:
            file_info = {
                'full_file_path': rf.full_file_path,
                'file_name': rf.file_name,
                'is_zip': rf.is_zip,
                'file_type': rf.file_type,
                'd': rf.d,
            }
            
            if args.verbose:
                file_info.update({
                    'member_name': rf.member_name,
                    'extract_vars': rf.extract_vars,
                    'd_formater': rf.d_formater,
                    'meta_data': {
                        k: str(v) if hasattr(v, 'isoformat') else v
                        for k, v in rf.meta_data.items()
                    }
                })
            
            output['raw_files'].append(file_info)
        
        print(json.dumps(output, indent=2))
    
    else:
        # Human-readable output
        print(f"Parquet file: {args.parq_file}")
        print(f"Table: {args.table}")
        print(f"\nFound {len(raw_files)} raw file(s):\n")
        
        for i, rf in enumerate(raw_files, 1):
            print(f"{i}. {rf.full_file_path}")
            
            if args.verbose:
                print(f"   File type: {rf.file_type}")
                print(f"   Date/Delta: {rf.d}")
                print(f"   Is zip: {rf.is_zip}")
                if rf.member_name:
                    print(f"   Member: {rf.member_name}")
                if rf.extract_vars:
                    print(f"   Extract vars: {rf.extract_vars}")
                print()
        
        if not raw_files:
            print("No raw files found matching the criteria.")
