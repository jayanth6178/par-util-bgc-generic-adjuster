import datetime as dt
import glob
import os
import re
import zipfile


class RawFileInfo:
    """Container for raw file metadata and extracted information.

    Holds all information about a discovered raw file, including paths, extracted
    date/time components, file metadata, and regex-extracted variables.
    Used by readers and adjusters to access file information during processing.

    Attributes:
        full_file_path (str): Complete path to the file. For zip files, format is
            "zip_path|member_name".
        full_file_name (str): Just the filename portion (last path component).
        is_zip (bool): Whether this file is within a zip file.
        extract_vars (dict): Dictionary of variables extracted from filename via regex
            capture groups.
        d_formater (dict): Dictionary of all date/time format components extracted
            from filename.
        meta_data (dict): File metadata including file_path, file_name, file_mtime,
            file_size, and for zip files: zfile_name, zfile_mtime, zfile_size, etc.
        file_type (str): Type of file pattern.
        creation_time (datetime.datetime): UTC timestamp when this RawFileInfo was created.
        file_name (str): For regular files, same as full_file_name. For zip files,
            the zip filename.
        member_name (Optional[str]): For zip files, the filename within the
            zip. None for regular files.
        d (str): The primary date/delta string for this file.
    """

    def __init__(
        self,
        full_file_path,
        is_zip,
        extract_vars,
        d_formater,
        meta_data,
        file_type,
        d,
        creation_time=dt.datetime.now(dt.timezone.utc),
    ):
        """Initializes a RawFileInfo object.

        Args:
            full_file_path (str): Complete path to file. Format "zip|member" for zip files.
            is_zip (bool): Whether this is a zip file.
            extract_vars (dict): Variables extracted from filename via regex.
            d_formater (dict): Date/time format components extracted from filename.
            meta_data (dict): File metadata.
            file_type (str): Pattern type.
            d (str): Primary date/delta identifier for this file.
            creation_time (datetime.datetime): UTC timestamp of creation.
        """

        self.full_file_path = full_file_path
        self.full_file_name = full_file_path.split("/")[-1]
        self.is_zip = is_zip
        self.extract_vars = extract_vars
        self.d_formater = d_formater
        self.meta_data = meta_data
        self.file_type = file_type
        self.creation_time = creation_time

        if is_zip:
            self.file_name = self.full_file_name.split("|")[0]
            self.member_name = self.full_file_name.split("|")[1]
        else:
            self.file_name = self.full_file_name
            self.member_name = None

        self.d = d

    def __str__(self):
        """Returns a formatted string representation of the file info.

        Returns:
            str: Multi-line string with all file information.
        """

        return (
            f"RawFileInfo: \n"
            f"    full_file_path: {self.full_file_path}\n"
            f"    file_name: {self.file_name}\n"
            f"    member_name: {self.member_name}\n"
            f"    is_zip: {self.is_zip}\n"
            f"    file_type: {self.file_type}\n"
            f"    d_formater: {self.d_formater}\n"
            f"    meta_data: {self.meta_data}\n"
            f"    extract_vars: {self.extract_vars}\n"
            f"    d: {self.d}"
        )


class FileFinder:
    """Discovers files matching path patterns and extracts metadata.

    Converts template patterns with date/time placeholders and regex capture groups
    into glob patterns (for file discovery) and regex patterns (for metadata extraction).
    Supports regular files, compressed files (.gz), and zip files.

    Attributes:
        file_template (str): Original template pattern.
        extract_vars (dict): Configuration for extracting variables from capture groups.
        search_params (dict): Additional search parameters for template formatting.
        before (Optional[str]): Optional end date for range filtering.
        after (Optional[str]): Optional start date for range filtering.
        file_type (str): Type of pattern.
        is_zip (bool): Whether pattern includes zip member (contains '|').
        glob_template (str): Glob pattern for file discovery.
        regex_template (str): Regex pattern for archive files or regular files.
        member_regex_template (Optional[str]): Regex pattern for zip members.
    """

    def __init__(self, file_template, search_params=None, extract_vars=None, before=None, after=None, file_type="date"):
        """Initializes a FileFinder with a template pattern.

        Args:
            file_template (str): Path template with placeholders and optional regex
                capture groups. For zip files, use "archive_path|member_pattern".
            search_params (Optional[dict]): Additional parameters for template formatting.
            extract_vars (Optional[dict]): Dictionary mapping variable names to
                ExtractVarConfig objects for extracting values from regex capture groups.
            before (Optional[str]): End date for filtering.
            after (Optional[str]): Start date for filtering.
            file_type (str): Type of pattern.
        """

        self.file_template = file_template
        self.extract_vars = {} if extract_vars is None else extract_vars
        self.search_params = {} if search_params is None else search_params
        self.before = before
        self.after = after
        self.file_type = file_type
        self.is_zip = "|" in file_template

        ## Validate search_params
        ## ========================================
        # Get all formatters from file_template
        template_keys = set(re.findall(r"\{(\w+)\}", self.file_template))

        # Define expected date/time keys that don't need to be in search_params
        expected_keys = {
            "YYYY",
            "MM",
            "DD",
            "YYYYMMDD",
            "YYYYMM",
            "YYMMDD",
            "YYMM",
            "YY",
            "hh",
            "mm",
            "ss",
            "ms",
            "us",
            "delta",
        }

        # Find keys in template that are not in search_params and not expected
        missing_keys = template_keys - set(self.search_params.keys()) - expected_keys

        if missing_keys:
            raise ValueError(f"You are missing the following required search_params: {missing_keys}")
        ## ========================================

        self.glob_template = self._template_to_glob()
        self.regex_template, self.member_regex_template = self._template_to_regex()

    def __str__(self):
        """Returns a formatted string representation of the finder configuration.

        Returns:
            str: multi-line string with all configuration details.
        """

        return (
            f"FileFinder: \n"
            f"  file_template: {self.file_template}\n"
            f"  glob_template: {self.glob_template}\n"
            f"  regex_template: {self.regex_template}\n"
            f"  member_regex_template: {self.member_regex_template}\n"
            f"  file_type: {self.file_type}\n"
            f"  is_zip: {self.is_zip}\n"
            f"  before: {self.before}\n"
            f"  after: {self.after}\n"
            f"  extract_vars: {self.extract_vars}\n"
            f"  search_params: {self.search_params}"
        )

    def _template_to_glob(self):
        """Converts a template pattern to a glob pattern for file discovery.

        Replaces date/time placeholders with glob wildcards and removes regex
        capture groups. For zip files, only processes the archive path (before '|').

        Returns:
            str: Glob pattern suitable for glob.glob().
        """

        # remove member file
        glob_pattern = self.file_template.split("|")[0]

        # remove parentheses
        glob_pattern = glob_pattern.replace("(", "").replace(")", "")

        # replace date and delta with wildcards
        glob_pattern = glob_pattern.format(
            YYYY="????",
            MM="??",
            DD="??",
            YYYYMMDD="????????",
            YYYYMM="??????",
            YYMMDD="??????",
            YYMM="??????",
            YY="??",
            hh="??",
            mm="??",
            ss="??",
            ms="???",
            us="??????",
            delta="*",
            **{k: "*" for k in self.search_params.keys()},
        )

        return glob_pattern

    def _template_to_regex(self):
        """Converts a template pattern to regex pattern(s) for metadata extraction.

        Replaces date/time placeholders with regex patterns and converts glob wildcards
        to regex equivalents. For zip files, returns separate patterns for the archive
        and member.

        Returns:
            tuple: (archive_regex, member_regex) where member_regex is None for regular files.
        """

        def _to_regex(s):

            # replace glob wildcards with regex wildcards
            s = s.replace(".", r"\.").replace("*", ".*").replace("?", ".")

            # format the date and delta with regex patterns
            s = s.format(
                YYYY="[0-9]{4}",
                MM="[0-9]{2}",
                DD="[0-9]{2}",
                YYYYMMDD="[0-9]{8}",
                YYYYMM="[0-9]{6}",
                YYMMDD="[0-9]{6}",
                YYMM="[0-9]{4}",
                YY="[0-9]{2}",
                hh="[0-9]{2}",
                mm="[0-9]{2}",
                ss="[0-9]{2}",
                ms="[0-9]{3}",
                us="[0-9]{6}",
                delta="[0-9]+",
                **{k: ".*" for k in self.search_params.keys()},
            )

            return s

        if self.is_zip:
            # split by member
            file = self.file_template.split("|")[0]
            member = self.file_template.split("|")[1]

            return _to_regex(file), _to_regex(member)

        else:
            return _to_regex(self.file_template), None

    def _extract_d_formater(self, file_path):
        """Extracts all date/time format components from a file path.

        Matches the file path against the template pattern and extracts date/time
        components (YYYY, MM, DD, etc.). Derives missing composite formats from
        component parts and vice versa.

        Args:
            file_path (str): Complete file path to extract from.

        Returns:
            dict: Dictionary with keys for all date/time formats.
        """

        # Step 1: Remove parentheses and apply regex patterns
        regex_pattern = self.file_template.replace("(", "").replace(")", "")
        regex_pattern = regex_pattern.replace(r"|", r"\|")  # Escape pipes
        regex_pattern = regex_pattern.replace(r".", r"\.")  # Escape dots
        regex_pattern = regex_pattern.replace(r"*", r".*")  # Convert globs
        regex_pattern = regex_pattern.replace(r"?", r".")  # Convert globs

        # Add search params to regex pattern
        for k, v in self.search_params.items():
            key_str = "{" + k + "}"
            regex_pattern = regex_pattern.replace(key_str, v)

        # Step 2: Find key patterns
        pattern_regex = r"\{(YYYYMMDD|YYYYMM|YYMMDD|YYMM|YYYY|MM|DD|YY|hh|mm|ss|ms|us|delta)\}"
        found_patterns = re.findall(pattern_regex, regex_pattern)

        # Define regex patterns for each date/delta type
        regex_mapping = {
            "YYYYMMDD": "([0-9]{8})",
            "YYYYMM": "([0-9]{6})",
            "YYMMDD": "([0-9]{6})",
            "YYMM": "([0-9]{4})",
            "YYYY": "([0-9]{4})",
            "MM": "([0-9]{2})",
            "DD": "([0-9]{2})",
            "YY": "([0-9]{2})",
            "hh": "([0-9]{2})",
            "mm": "([0-9]{2})",
            "ss": "([0-9]{2})",
            "ms": "([0-9]{3})",
            "us": "([0-9]{6})",
            "delta": "([0-9]+)",
        }

        # Replace each pattern with its regex equivalent
        for pattern_type in found_patterns:
            pattern_placeholder = "{" + pattern_type + "}"
            regex_pattern = regex_pattern.replace(pattern_placeholder, regex_mapping[pattern_type], 1)

        # Step 4: Match the regex against the filename
        match = re.match(regex_pattern, file_path)

        if not match:
            # Return default structure with all None values
            return {
                "YYYY": None,
                "MM": None,
                "DD": None,
                "YYYYMM": None,
                "YY": None,
                "YYMM": None,
                "YYMMDD": None,
                "YYYYMMDD": None,
                "hh": None,
                "mm": None,
                "ss": None,
                "ms": None,
                "us": None,
                "delta": None,
            }

        # Step 5: Extract captured groups and map them to pattern types
        captured_values = {}
        for i, pattern_type in enumerate(found_patterns):
            value = match.group(i + 1)
            captured_values[pattern_type] = value

        # Step 6: Build result with captured values
        result = {
            "YYYY": None,
            "MM": None,
            "DD": None,
            "YYYYMM": None,
            "YY": None,
            "YYMM": None,
            "YYMMDD": None,
            "YYYYMMDD": None,
            "hh": None,
            "mm": None,
            "ss": None,
            "ms": None,
            "us": None,
            "delta": None,
        }

        # Copy directly captured values
        for key in captured_values:
            result[key] = captured_values[key]

        # Step 7: Derived missing date formats from whatever we extracted
        # Extract individual components from composite formats
        if result["YYYYMMDD"] and not all([result["YYYY"], result["MM"], result["DD"]]):
            result["YYYY"] = result["YYYYMMDD"][:4]
            result["MM"] = result["YYYYMMDD"][4:6]
            result["DD"] = result["YYYYMMDD"][6:8]

        if result["YYYYMM"] and not all([result["YYYY"], result["MM"]]):
            result["YYYY"] = result["YYYYMM"][:4]
            result["MM"] = result["YYYYMM"][4:6]

        if result["YYMMDD"] and not all([result["YY"], result["MM"], result["DD"]]):
            result["YY"] = result["YYMMDD"][:2]
            result["MM"] = result["YYMMDD"][2:4]
            result["DD"] = result["YYMMDD"][4:6]

        if result["YYMM"] and not all([result["YY"], result["MM"]]):
            result["YY"] = result["YYMM"][:2]
            result["MM"] = result["YYMM"][2:4]

        # Convert YY to YYYY if we have YY but not YYYY
        if result["YY"] and not result["YYYY"]:
            result["YYYY"] = str(dt.datetime.strptime(result["YY"], "%y").year)

        # Convert YYYY to YY if we have YYYY but not YY
        if result["YYYY"] and not result["YY"]:
            result["YY"] = result["YYYY"][2:4]

        # Build composite formats from individual components
        if result["YYYY"] and result["MM"] and result["DD"] and not result["YYYYMMDD"]:
            result["YYYYMMDD"] = result["YYYY"] + result["MM"] + result["DD"]

        if result["YYYY"] and result["MM"] and not result["YYYYMM"]:
            result["YYYYMM"] = result["YYYY"] + result["MM"]

        if result["YY"] and result["MM"] and result["DD"] and not result["YYMMDD"]:
            result["YYMMDD"] = result["YY"] + result["MM"] + result["DD"]

        if result["YY"] and result["MM"] and not result["YYMM"]:
            result["YYMM"] = result["YY"] + result["MM"]

        return result

    def _extract_metadata(self, file_path):
        """Extracts file system metadata from a file path.

        Gets file size, modification time, and for zip files, both archive and
        member metadata.

        Args:
            file_path (str): Complete file path. For zip files, format is "zip_path|member".

        Returns:
            dict: Dictionary with keys: file_path, file_name, file_mtime, file_size,
                and for zip files: zfile_name, zfile_mtime, zfile_size, zfile_compress_size.
        """

        if not self.is_zip:
            return {
                "file_path": file_path,
                "file_name": os.path.basename(file_path),
                "file_mtime": dt.datetime.fromtimestamp(os.path.getmtime(file_path), dt.timezone.utc),
                "file_size": os.path.getsize(file_path),
                "zfile_name": None,
                "zfile_mtime": None,
                "zfile_size": None,
                "zfile_compress_size": None,
            }

        else:
            zfile_path = file_path.split("|")[0]
            mfile_name = file_path.split("|")[1]
            zfile_name = os.path.basename(zfile_path)
            with zipfile.ZipFile(zfile_path, "r") as zip_ref:
                mfile_info = zip_ref.getinfo(mfile_name)

            return {
                "file_path": file_path,
                "file_name": mfile_name,
                "file_mtime": dt.datetime(
                    *mfile_info.date_time
                ),  # NOTE: file_mtime does NOT have timezone due to the way zip files store mtime
                "file_size": mfile_info.file_size,
                "zfile_name": zfile_name,
                "zfile_mtime": dt.datetime.fromtimestamp(os.path.getmtime(zfile_path), dt.timezone.utc),
                "zfile_size": os.path.getsize(zfile_path),
                "zfile_compress_size": mfile_info.compress_size,
            }

    def _extract_vars(self, file_path):
        """Extracts variables from file path using regex capture groups.

        Matches the file path against the regex pattern and extracts values from
        capture groups, casting them to the configured types.

        Args:
            file_path (str): Complete file path to extract from.

        Returns:
            dict: Dictionary mapping variable names to their extracted and cast values.
        """

        # Determine which regex template to use
        if self.is_zip:
            regex_pattern = self.regex_template + r"\|" + self.member_regex_template
        else:
            regex_pattern = self.regex_template

        # Match the file path against the regex pattern
        match = re.match(regex_pattern, file_path)

        # If no match found, return empty dictionary
        if not match:
            return {}

        # Extract all captured groups
        captured_groups = match.groups()

        # Create dictionary mapping extract_vars to captured groups with type casting
        result = {}
        for var_name, var_config in self.extract_vars.items():
            regex_group = var_config.get("regex_group") if isinstance(var_config, dict) else var_config.regex_group
            dtype = var_config.get("dtype") if isinstance(var_config, dict) else var_config.dtype

            # Get the captured group value (regex groups are 1-indexed)
            if regex_group <= len(captured_groups):
                raw_value = captured_groups[regex_group - 1]

                # Cast to the specified dtype
                if raw_value is not None:
                    if dtype == "int":
                        result[var_name] = int(raw_value)
                    elif dtype == "float":
                        result[var_name] = float(raw_value)
                    elif dtype == "string" or dtype == "str":
                        result[var_name] = str(raw_value)
                    else:
                        # Default to string if dtype is unknown
                        result[var_name] = str(raw_value)
                else:
                    result[var_name] = None
            else:
                # If regex_group is out of range, set to None
                result[var_name] = None

        return result

    def _process_single_file(self, file_path, creation_time):
        """Processes a single file path and creates a RawFileInfo object if it matches.

        Args:
            file_path (str): Complete file path to process. For zip files, format is "zip_path|member".
            creation_time (datetime.datetime): Creation timestamp to use for the RawFileInfo.

        Returns:
            RawFileInfo or None: RawFileInfo object if file matches pattern, None otherwise.
        """
        d_formater_dict = self._extract_d_formater(file_path)
        metadata_dict = self._extract_metadata(file_path)
        extract_vars_dict = self._extract_vars(file_path)

        if self.file_type == "date":
            d = d_formater_dict["YYYYMMDD"]
        elif self.file_type == "delta":
            d = d_formater_dict["delta"]
        elif self.file_type == "month":
            d = d_formater_dict["YYYYMM"]
        else:
            d = None

        for search_param_item, search_param_value in self.search_params.items():
            extract_vars_dict[search_param_item] = search_param_value

        # ensure in range
        if d is not None:
            if self.before is not None and int(d) >= int(self.before):
                return None
            if self.after is not None and int(d) <= int(self.after):
                return None

        return RawFileInfo(
            file_path,
            self.is_zip,
            extract_vars_dict,
            d_formater_dict,
            metadata_dict,
            self.file_type,
            d,
            creation_time=creation_time,
        )

    def process_raw_files(self, all_f):
        """Processes a list of file paths and extracts metadata for each.

        For each file, extracts date components, file metadata, and regex variables.
        For zip files, processes all matching members within each archive.
        Filters files based on before/after date ranges if configured.

        Args:
            all_f (list): List of file paths to process.

        Returns:
            list: List of RawFileInfo objects for all matching files.
        """

        found_files = list()
        creation_time = dt.datetime.now(dt.timezone.utc)

        # loop through all files
        for raw_file in all_f:
            # must process slightly differently for zip files
            if self.is_zip:
                with zipfile.ZipFile(raw_file, "r") as zip_ref:
                    # List all files in the zip
                    zip_members = zip_ref.namelist()

                # Check each member against the member regex pattern
                for member in zip_members:
                    if re.match(self.member_regex_template, member):
                        # This member matches the pattern
                        member_file_path = f"{raw_file}|{member}"
                        file_info = self._process_single_file(member_file_path, creation_time)
                        if file_info is not None:
                            found_files.append(file_info)

            else:
                file_info = self._process_single_file(raw_file, creation_time)
                if file_info is not None:
                    found_files.append(file_info)

        return found_files

    def find_all(self):
        """Finds all files matching the template pattern.

        If before/after ranges are configured, uses find_range(). Otherwise,
        uses glob to find all matching files.

        Returns:
            list: List of RawFileInfo objects for all discovered files.
        """

        if self.before and self.after:
            return self.find_range(self.before, self.after)

        all_f = glob.glob(self.glob_template)

        return self.process_raw_files(all_f)

    def find_range(self, after, before):
        """Finds files within a specific range.

        Iterates through the range and finds files for each date.
        More efficient than find_all() for large date ranges as it generates specific
        paths rather than globbing everything.

        Args:
            after (str): Start date/delta/month.
            before (str): End date/delta/month.

        Returns:
            list: List of RawFileInfo objects for all files in the range.
        """

        found_files = list()

        # remove member file
        file_template = self.file_template.split("|")[0]

        # remove parentheses
        file_template = file_template.replace("(", "").replace(")", "")

        if self.file_type == "date" or self.file_type == "month":
            if self.file_type == "date":
                start = dt.datetime.strptime(after, "%Y%m%d")
                end = dt.datetime.strptime(before, "%Y%m%d")
            else:
                start = dt.datetime.strptime(after + "01", "%Y%m%d")
                end = dt.datetime.strptime(before + "01", "%Y%m%d")

            current = start
            while current <= end:

                curr_glob_template = file_template.format(
                    YYYY=current.strftime("%Y"),
                    MM=current.strftime("%m"),
                    DD=current.strftime("%d"),
                    YYYYMMDD=current.strftime("%Y%m%d"),
                    YYYYMM=current.strftime("%Y%m"),
                    YYMMDD=current.strftime("%y%m%d"),
                    YYMM=current.strftime("%y%m"),
                    YY=current.strftime("%y"),
                    hh="??",
                    mm="??",
                    ss="??",
                    ms="???",
                    us="??????",
                    delta="*",
                    **self.search_params,
                )
                all_f = glob.glob(curr_glob_template)

                found_files.extend(self.process_raw_files(all_f))

                if self.file_type == "date":
                    current += dt.timedelta(days=1)
                else:
                    if current.month < 12:
                        current = current.replace(month=current.month + 1)
                    else:
                        current = current.replace(year=current.year + 1, month=1)

        # delta files
        else:
            start = int(after)
            end = int(before)

            for d in range(start, end + 1):
                curr_glob_template = file_template = file_template.format(
                    YYYY="????",
                    MM="??",
                    DD="??",
                    YYYYMMDD="????????",
                    YYYYMM="??????",
                    YYMMDD="??????",
                    YYMM="??????",
                    YY="??",
                    hh="??",
                    mm="??",
                    ss="??",
                    ms="???",
                    us="??????",
                    delta=d,
                    **self.search_params,
                )

                all_f = glob.glob(curr_glob_template)

                found_files.extend(self.process_raw_files(all_f))

        return found_files
