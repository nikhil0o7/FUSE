import os
import errno
import stat
import sys
import subprocess
from fuse import FUSE, Operations, FuseOSError
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.api_core.exceptions import NotFound
#import gcsfuse

class GCSFS(Operations):
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.fd = 0
        self.open_files = {}

    def mount(self,bucket_name, mount_folder):
        try:
        # Make sure the mount directory exists
            os.makedirs(mount_folder, exist_ok=True)

        # Run the gcsfuse command
            subprocess.run(['gcsfuse', bucket_name, mount_folder], check=True)
            print(f"Bucket {bucket_name} mounted successfully.")
        except subprocess.CalledProcessError as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def create(self, path, mode):
        # Create an empty file
        blob = self.bucket.blob(path.lstrip('/'))
        blob.upload_from_string('')
        return 0

    def close(self, path):
    # Search for the path in the open_files dictionary to find the corresponding file handle (fh)
        found_fh = None
        for fh, file_info in self.open_files.items():
            if file_info['path'] == path:
                found_fh = fh