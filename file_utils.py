import bz2
import gzip
import lzma
import os
from typing import BinaryIO, Optional


def create_dir(file_path):
    dirname = os.path.dirname(file_path)
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)


def detect_compression_type(file_path: str) -> Optional[str]:
    """
    Detect compression type by reading file signature (magic numbers).
    Falls back to extension-based detection if signature detection fails.

    Args:
        file_path: Path to the file

    Returns:
        Compression type: 'gzip', 'bz2', 'xz', or None for uncompressed

    Magic numbers:
        - gzip: 1f 8b
        - bzip2: 42 5a (BZ)
        - xz: fd 37 7a 58 5a 00 (�7zXZ\x00)
    """
    try:
        with open(file_path, "rb") as f:
            magic = f.read(6)  # Read first 6 bytes to cover all signatures

        if len(magic) >= 2:
            # Check gzip (1f 8b)
            if magic[0:2] == b"\x1f\x8b":
                return "gzip"

            # Check bzip2 (42 5a - "BZ")
            if magic[0:2] == b"\x42\x5a" or magic[0:2] == b"BZ":
                return "bz2"

        if len(magic) >= 6:
            # Check xz (fd 37 7a 58 5a 00)
            if magic[0:6] == b"\xfd\x37\x7a\x58\x5a\x00" or magic[0:6] == b"\xfd7zXZ\x00":
                return "xz"
    except Exception:
        # If we can't read the file, fall back to extension-based detection
        pass

    # Fall back to extension-based detection if signature detection fails or finds nothing
    path_lower = file_path.lower()
    if path_lower.endswith(".gz"):
        return "gzip"
    elif path_lower.endswith(".bz2"):
        return "bz2"
    elif path_lower.endswith((".xz", ".lzma")):
        return "xz"

    return None


def open_file_raw(file_path: str, mode: str = "rt", encoding: Optional[str] = None) -> BinaryIO:
    """
    Open a file in raw mode with support for various compression formats.

    Automatically detects compression type by reading file signatures (magic numbers).
    Falls back to extension-based detection if signature detection fails.

    Args:
        file_path: Path to the file
        mode: File open mode ('rb' or 'rt')
        encoding: Text encoding (only used if mode is 'rt')

    Returns:
        A file handler that can read raw bytes or text

    Supports:
        - gzip (magic: 1f 8b)
        - bzip2 (magic: 42 5a)
        - lzma/xz (magic: fd 37 7a 58 5a 00)
        - uncompressed files
    """
    # Detect compression type (handles both signature and extension fallback)
    compression_type = detect_compression_type(file_path)

    # Open file based on detected compression type
    if compression_type == "gzip":
        if mode == "rt":
            return gzip.open(file_path, mode, encoding=encoding)
        return gzip.open(file_path, mode)
    elif compression_type == "bz2":
        if mode == "rt":
            return bz2.open(file_path, mode, encoding=encoding)
        return bz2.open(file_path, mode)
    elif compression_type == "xz":
        if mode == "rt":
            return lzma.open(file_path, mode, encoding=encoding)
        return lzma.open(file_path, mode)
    else:
        # Regular uncompressed file
        if mode == "rt":
            return open(file_path, mode, encoding=encoding)
        return open(file_path, mode)
