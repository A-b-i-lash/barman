# -*- coding: utf-8 -*-
# © Copyright EnterpriseDB UK Limited 2018-2021
#
# This file is part of Barman.
#
# Barman is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Barman is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Barman.  If not, see <http://www.gnu.org/licenses/>.

"""
The module handles compression specifics for barman-cloud so that compression
algorithms requiring external dependencies can be lazily-imported and therefore
treated as optional requirements.
"""

import bz2
import gzip
import shutil
from io import BytesIO


def _try_import_snappy():
    """Lazily import the snappy library."""
    try:
        import snappy
    except ImportError:
        raise SystemExit("Missing required python module: python-snappy")
    return snappy


def compress(wal_file, compression):
    """
    Compresses the supplied wal_file and returns a file-like object containing the
    compressed data.
    :param IOBase wal_file: A file-like object containing the WAL file data.
    :param str compression: The compression algorithm to apply. Can be one of:
      bzip2, gzip, snappy.
    """
    if compression == "snappy":
        snappy = _try_import_snappy()
        in_mem_snappy = BytesIO()
        snappy.stream_compress(wal_file, in_mem_snappy)
        in_mem_snappy.seek(0)
        return in_mem_snappy
    elif compression == "gzip":
        # Create a BytesIO for in memory compression
        in_mem_gzip = BytesIO()
        with gzip.GzipFile(fileobj=in_mem_gzip, mode="wb") as gz:
            # copy the gzipped data in memory
            shutil.copyfileobj(wal_file, gz)
        in_mem_gzip.seek(0)
        return in_mem_gzip
    elif compression == "bzip2":
        # Create a BytesIO for in memory compression
        in_mem_bz2 = BytesIO(bz2.compress(wal_file.read()))
        in_mem_bz2.seek(0)
        return in_mem_bz2
    else:
        raise ValueError("Unknown compression type: %s" % compression)


def get_streaming_tar_mode(mode, compression):
    """
    Helper function used in streaming uploads and downloads which appends the supplied
    compression to the raw filemode (either r or w) and returns the result. Any
    compression algorithms supported by barman-cloud but not Python TarFile are
    ignored so that barman-cloud can apply them itself.

    :param str mode: The file mode to use, either r or w.
    :param str compression: The compression algorithm to use. Can be set to snappy
     or any compression supported by the TarFile mode string.
    """
    if compression == "snappy" or compression is None:
        return "%s|" % mode
    else:
        return "%s|%s" % (mode, compression)


def get_decompressor(compression):
    """
    Helper function which returns a streaming decompressor for the specified
    compression algorithm. Currently only snappy is supported. Other compression
    algorithms use the decompression built into TarFile.

    :param str compression: The compression algorithm to use. Can be set to snappy
     or any compression supported by the TarFile mode string.
    """
    if compression == "snappy":
        snappy = _try_import_snappy()
        return snappy.StreamDecompressor()
    return None


def get_compressor(compression):
    """
    Helper function which returns a streaming compressor for the specified
    compression algorithm. Currently only snappy is supported. Other compression
    algorithms use the decompression built into TarFile.

    :param str compression: The compression algorithm to use. Can be set to snappy
     or any compression supported by the TarFile mode string.
    """
    if compression == "snappy":
        snappy = _try_import_snappy()
        return snappy.StreamCompressor()
    return None


def decompress_to_file(blob, dest_file, compression):
    """
    Decompresses the supplied blob of data into the dest_file file-like object using
    the specified compression.

    :param IOBase blob: A file-like object containing the compressed data.
    :param IOBase dest_file: A file-like object into which the uncompressed data
      should be written.
    :param str compression: The compression algorithm to apply. Can be one of:
      bzip2, gzip, snappy.
    """
    if compression == "snappy":
        snappy = _try_import_snappy()
        snappy.stream_decompress(blob, dest_file)
        return
    elif compression == "gzip":
        source_file = gzip.GzipFile(fileobj=blob, mode="rb")
    elif compression == "bzip2":
        source_file = bz2.BZ2File(blob, "rb")
    else:
        raise ValueError("Unknown compression type: %s" % compression)

    with source_file:
        shutil.copyfileobj(source_file, dest_file)
