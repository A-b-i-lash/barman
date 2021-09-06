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
# along with Barman.  If not, see <http://www.gnu.org/licenses/>

import bz2
import gzip
import logging
import os
import shutil
from io import BytesIO, RawIOBase

from barman.cloud import CloudInterface

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    from google.cloud import storage
except ImportError:
    raise SystemExit("Missing required python module: google-cloud-storage")

from google.api_core.exceptions import GoogleAPIError

URL = "https://console.cloud.google.com/storage/browser"


class GoogleCloudInterface(CloudInterface):
    # Azure block blob limitations
    # https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
    MAX_CHUNKS_PER_FILE = 50000
    # Minimum block size allowed in Azure Blob Storage is 64KB
    MIN_CHUNK_SIZE = 64 << 10

    # Azure Blob Storage permit a maximum of 4.75TB per file
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    def __init__(self, url, jobs=2, encryption_scope=None):
        """
        Create a new Google cloud Storage interface given the supplied account url

        :param str url: Full URL of the cloud destination/source (ex: )
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 2.
        """
        super(GoogleCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
        )
        self.encryption_scope = encryption_scope

        parsed_url = urlparse(url)
        if not url.startswith(URL):
            msg = f"google cloud storage URL {url} is malformed. Should start with '{URL}'"
            raise ValueError(msg)
        self.account_url = parsed_url.netloc
        try:
            self.bucket_name = parsed_url.path.split("/")[3]
        except IndexError:
            raise ValueError(
                f"Google cloud storage URL {url} is malformed. Bucket name not found"
            )
        path = parsed_url.path.split("/")[4:]
        self.path = "/".join(path)

        self.bucket_exists = None
        self._reinit_session()

    def _reinit_session(self):
        """
        Create a new session
        Creates a client using "GOOGLE_APPLICATION_CREDENTIALS" env
        """
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/didier.michel/Downloads/barman-324718-e759c283753d.json'
        self.container_client = storage.Client().bucket(self.bucket_name)

    def test_connectivity(self):
        """
        Test gcs connectivity by trying to access a container
        """
        try:
            # We are not even interested in the existence of the bucket,
            # we just want to see if google cloud storage is reachable.
            self.bucket_exists = self._check_bucket_existence()

            return True
        except GoogleAPIError as exc:
            logging.error("Can't connect to cloud provider: %s", exc)
            return False

    def _check_bucket_existence(self):
        """
        Check google bucket

        :return: True if the container exists, False otherwise
        :rtype: bool
        """
        return self.container_client.exists()

    def _create_bucket(self):
        """
        Create the bucket in cloud storage
        """
        # Todo
        #  There are several parameters to manage (
        #  * project id ()
        #  * data location (default US multi-region)
        #  * data storage class (default standard)
        #  * access control
        #  Detailed documentation: https://googleapis.dev/python/storage/latest/client.html
        # Might be relevant to use configuration file for those parameters.
        self.container_client = self.container_client.client.create_bucket(
            self.container_client
        )

    # @abstractmethod
    def list_bucket(self, prefix="", delimiter="/"):
        """
        List bucket content in a directory manner

        :param str prefix:
        :param str delimiter:
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        pass

    # @abstractmethod
    def download_file(self, key, dest_path, decompress):
        """
        Download a file from cloud storage

        :param str key: The key identifying the file to download
        :param str dest_path: Where to put the destination file
        :param bool decompress: Whenever to decompress this file or not
        """
        pass

    # @abstractmethod
    def remote_open(self, key):
        """
        Open a remote object in cloud storage and returns a readable stream

        :param str key: The key identifying the object to open
        :return: A file-like object from which the stream can be read or None if
          the key does not exist
        """
        pass

    # @abstractmethod
    def upload_fileobj(self, fileobj, key):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        """
        pass

    # @abstractmethod
    def create_multipart_upload(self, key):
        """
        Create a new multipart upload and return any metadata returned by the
        cloud provider.

        This metadata is treated as an opaque blob by CloudInterface and will
        be passed into the _upload_part, _complete_multipart_upload and
        _abort_multipart_upload methods.

        The implementations of these methods will need to handle this metadata in
        the way expected by the cloud provider.

        Some cloud services do not require multipart uploads to be explicitly
        created. In such cases the implementation can be a no-op which just
        returns None.

        :param key: The key to use in the cloud service
        :return: The multipart upload metadata
        :rtype: dict[str, str]|None
        """
        pass

    # @abstractmethod
    def _upload_part(self, upload_metadata, key, body, part_number):
        """
        Upload a part into this multipart upload and return a dict of part
        metadata. The part metadata must contain the key "PartNumber" and can
        optionally contain any other metadata available (for example the ETag
        returned by S3).

        The part metadata will included in a list of metadata for all parts of
        the upload which is passed to the _complete_multipart_upload method.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param object body: A stream-like object to upload
        :param int part_number: Part number, starting from 1
        :return: The part metadata
        :rtype: dict[str, None|str]
        """
        pass

    # @abstractmethod
    def _complete_multipart_upload(self, upload_metadata, key, parts_metadata):
        """
        Finish a certain multipart upload

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        :param List[dict] parts_metadata: The list of metadata for the parts
          composing the multipart upload. Each part is guaranteed to provide a
          PartNumber and may optionally contain additional metadata returned by
          the cloud provider such as ETags.
        """
        pass

    # @abstractmethod
    def _abort_multipart_upload(self, upload_metadata, key):
        """
        Abort a certain multipart upload

        The implementation of this method should clean up any dangling resources
        left by the incomplete upload.

        :param dict upload_metadata: Provider-specific metadata for this upload
          e.g. the multipart upload handle in AWS S3
        :param str key: The key to use in the cloud service
        """
        pass

    # @abstractmethod
    def delete_objects(self, paths):
        """
        Delete the objects at the specified paths

        :param List[str] paths:
        """
        pass
