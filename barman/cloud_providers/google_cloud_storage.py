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
import re
import os
import shutil
from io import BytesIO, RawIOBase

from barman.cloud import CloudInterface, DEFAULT_DELIMITER

try:
    # Python 3.x
    from urllib.parse import urlparse
except ImportError:
    # Python 2.x
    from urlparse import urlparse

try:
    from google.cloud import storage
    from google.api_core.exceptions import GoogleAPIError
except ImportError:
    raise SystemExit("Missing required python module: google-cloud-storage")


BASE_URL = "https://console.cloud.google.com/storage/browser/"


class GoogleCloudInterface(CloudInterface):
    # https://cloud.google.com/storage/docs/xml-api/put-object-multipart?hl=en
    MAX_CHUNKS_PER_FILE = 1

    # https://github.com/googleapis/python-storage/blob/main/google/cloud/storage/blob.py#L3759
    # chunk_size for writes must be exactly a multiple of 256KiB as with
    # other resumable uploads. The default is 40 MiB.
    MIN_CHUNK_SIZE = 1 << 40

    # Azure Blob Storage permit a maximum of 4.75TB per file
    # This is a hard limit, while our upload procedure can go over the specified
    # MAX_ARCHIVE_SIZE - so we set a maximum of 1TB per file
    MAX_ARCHIVE_SIZE = 1 << 40

    def __init__(self, url, jobs=1, encryption_scope=None, profile_name=None):
        """
        Create a new Google cloud Storage interface given the supplied account url

        :param str url: Full URL of the cloud destination/source (ex: )
        :param int jobs: How many sub-processes to use for asynchronous
          uploading, defaults to 1.
        :param str encryption_scope: Todo: Not sure we need this unless user wants to use its own encryption key
        """
        self.bucket_name, self.path = self._parse_url(url)

        super(GoogleCloudInterface, self).__init__(
            url=url,
            jobs=jobs,
        )
        self.encryption_scope = encryption_scope

        self.profile_name = profile_name

        self.bucket_exists = None
        self._reinit_session()

    @staticmethod
    def _parse_url(url):
        """
        Parse url and return bucket name and path. Raise ValueError otherwise.
        """
        if not url.startswith(BASE_URL) and not url.startswith("gs://"):
            msg = "Google cloud storage URL {} is malformed. Expected format are '{}' or '{}'".format(
                url,
                os.path.join(BASE_URL, "bucket-name/some/path"),
                "gs://bucket-name/some/path",
            )
            raise ValueError(msg)
        gs_url = url.replace(BASE_URL, "gs://")
        parsed_url = urlparse(gs_url)
        if not parsed_url.netloc:
            raise ValueError(
                "Google cloud storage URL {} is malformed. Bucket name not found".format(
                    url
                )
            )
        return parsed_url.netloc, parsed_url.path.strip("/")

    def _reinit_session(self):
        """
        Create a new session
        Creates a client using "GOOGLE_APPLICATION_CREDENTIALS" env
        """
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/didier.michel/Downloads/barman-324718-e759c283753d.json'
        self.client = storage.Client()
        self.container_client = self.client.bucket(self.bucket_name)

        # # todo clean duplicate
        # session = boto3.Session(profile_name=self.profile_name)
        # self.s3 = session.resource("s3", endpoint_url=self.endpoint_url)

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
        self.client.create_bucket(self.container_client)

    def list_bucket(self, prefix="", delimiter=DEFAULT_DELIMITER):
        """
        List bucket content in a directory manner

        :param str prefix: Prefix used to filter blobs
        :param str delimiter: Delimiter, used with prefix to emulate hierarchy
        :return: List of objects and dirs right under the prefix
        :rtype: List[str]
        """
        logging.debug("list_bucket: {}, {}".format(prefix, delimiter))
        blobs = self.client.list_blobs(
            self.container_client, prefix=prefix, delimiter=delimiter
        )
        objects = list(map(lambda blob: blob.name, blobs))
        dirs = list(blobs.prefixes)
        logging.debug("objects {}".format(objects))
        logging.debug("dirs {}".format(dirs))
        return objects + dirs

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
        blob = self.container_client.get_blob(key)
        if blob is None:
            logging.debug("Key: {} does not exist".format(key))
            return None
        # todo: maybe open with rb ?
        return blob.open("r")

    def upload_fileobj(self, fileobj, key):
        """
        Synchronously upload the content of a file-like object to a cloud key

        :param fileobj IOBase: File-like object to upload
        :param str key: The key to identify the uploaded object
        """
        logging.info("upload_fileobj to {}".format(key))
        blob = self.container_client.blob(key)
        logging.info("blob initiated")
        try:
            blob.upload_from_file(fileobj)
        except Exception as e:
            logging.error(type(e))
            logging.error(e.__dict__)
            logging.error(e.with_traceback())
            raise e

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

        # # todo: duplicate
        # return self.s3.meta.client.create_multipart_upload(
        #     Bucket=self.bucket_name, Key=key, **self._extra_upload_args
        # )
        logging.info("Create_multipart_upload")
        return []

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
        logging.info("_upload_part")
        # https://googleapis.dev/python/google-resumable-media/latest/resumable_media/requests.html#multipart-uploads
        # this one manages splitting

        # blob = self.container_client.blob(key)
        # blob_writer = blob.open("rb")
        self.upload_fileobj(body, key)
        logging.info("_upload_part_done")
        return {
            "PartNumber": part_number,
        }

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
        # Nothing to do here
        logging.info("_complete_multipart_upload")
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
        # Probably delete things here in case it has already been uploaded ?
        # Maybe catch some exceptions like file not found (equivalent)
        self.delete_objects(key)
        pass

    # @abstractmethod
    def delete_objects(self, paths):
        """
        Delete the objects at the specified paths

        :param List[str] paths:
        """
        failures = {}
        for path in list(set(paths)):
            try:
                blob = self.container_client.blob(path)
                blob.delete()
            except GoogleAPIError as e:
                failures[path] = e

        if failures:
            raise RuntimeError("blabla")
        pass
