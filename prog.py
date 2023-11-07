# import FUSE as fuse
import time
import fuse
from errno import ENOENT
from stat import S_IFDIR, S_IFREG
from fuse import FUSE, FuseOSError, Operations
from google.cloud import storage
from google.cloud import storage as gcs
import logging



# Initialize Google Cloud Storage client
client = gcs.Client().from_service_account_json("nikhil.json")
bucket = client.get_bucket('fuse_ecc')

class GCSFS(Operations):
    def __init__(self):
        self.fd = 0

    def getattr(self, path, fh=None):
        # Default attributes
        default_attr = dict(st_mode=(S_IFDIR | 0o755), st_nlink=2,
                            st_size=0, st_ctime=time.time(), st_mtime=time.time(),
                            st_atime=time.time())
        if path == '/':
            return default_attr
        
        blob = bucket.get_blob(path.strip('/'))
        if blob is None:
            raise FuseOSError(ENOENT)
        
        # Set the 'st_mode' to a file or directory
        mode = S_IFDIR if blob.name.endswith('/') else S_IFREG
        attr = default_attr
        attr['st_mode'] = mode | 0o666  # Set read-only permissions
        attr['st_size'] = blob.size
        return attr

    def create(self, path, mode):
        self.fd += 1
        try:
            blob = bucket.blob(path.strip('/'))
            blob.upload_from_string('')  # Create an empty blob (file)
            logging.info(f"Created file: {path}")
        except Exception as e:
            logging.error(f"Failed to create file {path}: {e}")
            raise FuseOSError(ENOENT)  # Or appropriate error code
        return self.fd
#         self.fd += 1
#         blob = bucket.blob(path.strip('/'))
#         blob.upload_from_string('')  # Create an empty blob (file)
#         return self.fd
            

    # ... other methods would need to be implemented ...
if __name__ == '__main__':
    # import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument('mount')
    # args = parser.parse_args()

    fuse = FUSE(GCSFS(), "./point", foreground=True, allow_other=True)