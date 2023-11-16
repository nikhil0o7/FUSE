import os
import errno
import stat
import sys
import subprocess
from fuse import FUSE, Operations, FuseOSError
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.api_core.exceptions import NotFound
import time
import logging

class GCSFS(Operations):
    def __init__(self, bucket_name):
        self.client = storage.Client().from_service_account_json('nikhil.json')
        self.bucket = self.client.bucket(bucket_name)
        self.fd = 0
        self.open_files = {}
        
    def opendir(self, path):
        gcs_path = path.lstrip('/').rstrip('/')
        blobs = list(self.bucket.list_blobs(prefix=gcs_path, delimiter='/', max_results=1))
        if not blobs and gcs_path:
            raise FuseOSError(errno.ENOENT)
        return 0

    def rmdir(self, path):
        gcs_path = path.lstrip('/').rstrip('/') + '/'
        blobs = list(self.bucket.list_blobs(prefix=gcs_path))
        if any(b.name != gcs_path for b in blobs): 
            raise FuseOSError(errno.ENOTEMPTY)
        else:
            dir_blob = self.bucket.blob(gcs_path)
            if dir_blob.exists():
                dir_blob.delete()
            return 0 
        
    def remove_directory_contents(self, directory_path):
        blobs = self.bucket.list_blobs(prefix=directory_path)
        for blob in blobs:
            self.bucket.delete_blob(blob.name)
            print(f"Deleted: {blob.name}")

    def rmdir(self, path):
        directory_path = path.strip('/') + '/'  
        try:
            blobs = list(self.bucket.list_blobs(prefix=directory_path))
            if any(blob.name != directory_path for blob in blobs):
                print(f"Directory {path} is not empty, deleting all the files in the give dir")
                self.remove_directory_contents(directory_path)
                return
#                 raise FuseOSError(errno.ENOTEMPTY)  # Directory not empty error

            directory_blob = self.bucket.blob(directory_path)
            if directory_blob.exists():
                self.bucket.delete_blob(directory_blob.name)
                print(f"Deleted directory marker for {path}")
            else:
                print(f"Directory {path} does not exist or is already deleted.")
                raise FuseOSError(errno.ENOENT) 

        except NotFound:
            print(f"Directory {path} does not exist.")
            raise FuseOSError(errno.ENOENT)  # No such file or directory error
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise FuseOSError(errno.EIO)  # Input/output error
    

    def listxattr(self, path):
        gcs_path = path.lstrip('/')
        blob = self.bucket.blob(gcs_path)
        try:
            blob.reload()  
            xattrs = blob.metadata or {}
            return list(xattrs.keys())
        except NotFound:
            raise FuseOSError(errno.ENOENT)
    
    def open(self, path, flags):
        self.fd += 1
        self.open_files[self.fd] = {'path': path, 'flags': flags}
        return self.fd
    
    def getattr(self, path, fh=None):
        if path == "/":
            return {
                'st_mode': (stat.S_IFDIR | 0o755),
                'st_nlink': 2
            }
        else:
            gcs_path = path.lstrip('/')
            try:
                blob = self.bucket.get_blob(gcs_path)
                if blob:
                    return {
                        'st_mode': (stat.S_IFREG | 0o644),
                        'st_nlink': 1,
                        'st_size': blob.size,
                        'st_ctime': time.mktime(blob.time_created.timetuple()),
                        'st_mtime': time.mktime(blob.updated.timetuple()),
                        'st_atime': time.mktime(blob.updated.timetuple())
                    }
                else:
                    if list(self.bucket.list_blobs(prefix=gcs_path, max_results=1)):
                        return {
                            'st_mode': (stat.S_IFDIR | 0o755),
                            'st_nlink': 2
                        }
                    else:
                        raise FuseOSError(errno.ENOENT)
            except NotFound:
                raise FuseOSError(errno.ENOENT)

                
    def getxattr(self, path, name, position=0):
        if path == "/":
            # Root directory has no xattrs
            raise FuseOSError(errno.ENODATA)
        gcs_path = path.lstrip('/')
        blob = self.bucket.blob(gcs_path)
        try:
            blob.reload()
            xattrs = blob.metadata or {}
            if name in xattrs:
                return xattrs[name]
            else:
                raise FuseOSError(errno.ENODATA)
        except NotFound:
            raise FuseOSError(errno.ENOENT)
            
            
    def readdir(self, path, fh):
        """Read a directory. Returns a list of directory contents."""
        if not path.endswith('/'):
            path += '/'
        path = path.lstrip('/')

        contents = ['.', '..']  # Default entries
        # Use prefix to list blobs in the specified 'path' and delimiter to not traverse subdirectories
        iterator = self.bucket.list_blobs(prefix=path, delimiter='/')
        blobs = list(iterator)
        prefixes = set(iterator.prefixes)

        for blob in blobs:
            # Extract the part of the blob name that is beyond the specified 'path'
            relative_path = blob.name[len(path):]
            if '/' not in relative_path:
                name = os.path.basename(blob.name)
                if name:  # Avoid adding empty names
                    contents.append(name)

        for prefix in prefixes:
            subdir = os.path.basename(prefix.rstrip('/'))
            if subdir:
                contents.append(subdir)

        return contents



    def read(self, path, size, offset, fh):
        gcs_path = path.lstrip('/')
        blob = self.bucket.blob(gcs_path)
        file_data = blob.download_as_string()
        return file_data[offset:offset + size]

    
    def write(self, path, data, offset, fh):
        """Write data to a file."""
        gcs_path = path.lstrip('/')
        blob = self.bucket.blob(gcs_path)
        file_data = blob.download_as_string()
        new_data = file_data[:offset] + data + file_data[offset + len(data):]
        blob.upload_from_string(new_data, content_type='text/plain')
        return len(data)

    def release(self, path, fh):
        if fh in self.open_files:
            del self.open_files[fh]
        return 0
    
    def mkdir(self, path, mode):
        directory_path = path.rstrip('/') + '/'
        directory_blob = self.bucket.blob(directory_path.lstrip('/'))
        try:
            directory_blob.upload_from_string('')
            return 0  # Success should return 0
        except Exception as e:
            print(f"An error occurred while creating directory: {e}")
            raise FuseOSError(errno.EIO)  # Input/output error
            
    def list_blobs(self):
        blobs = self.client.list_blobs(self.bucket)
        for blob in blobs:
            print(blob.name)

    
    def unlink(self, path):
        gcs_path = path.lstrip('/')
        blob = self.bucket.blob(gcs_path)
        try:
            blob.delete()
            return 0  # Success should return 0
        except NotFound:
            raise FuseOSError(errno.ENOENT)  # No such file or directory
        except Exception as e:
            print(f"An error occurred while deleting file: {e}")
            raise FuseOSError(errno.EIO)  # Input/output error

    def rename(self, old, new):
        old_gcs_path = old.lstrip('/')
        new_gcs_path = new.lstrip('/')
        old_blob = self.bucket.blob(old_gcs_path)
        new_blob = self.bucket.blob(new_gcs_path)
        try:
            # Copy the old blob to the new location
            self.bucket.copy_blob(old_blob, self.bucket, new_blob.name)
            # Delete the old blob
            old_blob.delete()
            return 0  # Success should return 0
        except NotFound:
            raise FuseOSError(errno.ENOENT)  # No such file or directory
        except Exception as e:
            print(f"An error occurred while renaming file: {e}")
            raise FuseOSError(errno.EIO)  # Input/output error
        

#     def create(self, path, mode, fi=None):
#         self.fd += 1
#         gcs_path = path.lstrip('/')
#         if not gcs_path:
#             logging.error("Path is empty, cannot create blob.")
#             raise FuseOSError(errno.EINVAL)  # Invalid argument

#         blob = self.bucket.blob(gcs_path)

#         try:
#             if blob.exists():
#                 logging.error(f"Blob {gcs_path} already exists.")
#                 raise FuseOSError(errno.EEXIST)  # File exists

#             blob.upload_from_string('', content_type='text/plain')
#             self.open_files[self.fd] = {'path': gcs_path, 'flags': 'w'}  # 'w' for write access
#             logging.debug(f"Created blob {gcs_path} with fd {self.fd}")
#         except Exception as e:
#             logging.error(f"An error occurred while creating file: {e}")
#             raise FuseOSError(errno.EIO) 

#         return self.fd

    def create(self, path, mode, fi=None):
        self.fd += 1
        gcs_path = path.lstrip('/')
        
        if not gcs_path:
            logging.error("Path is empty, cannot create blob.")
            raise FuseOSError(errno.EINVAL)  # Invalid argument

        blob = self.bucket.blob(gcs_path)

        try:
            if blob.exists():
                logging.error(f"Blob {gcs_path} already exists.")
                raise FuseOSError(errno.EEXIST)  # File exists

            # Ensure we are creating a file, not a directory
            if gcs_path.endswith('/'):
                logging.error("Attempted to create a directory instead of a file.")
                raise FuseOSError(errno.EISDIR)  # Is a directory

            # Creating an empty file
            blob.upload_from_string('', content_type='text/plain')
            self.open_files[self.fd] = {'path': gcs_path, 'flags': 'w'}  # 'w' for write access
            logging.debug(f"Created blob {gcs_path} with fd {self.fd}")
        except Exception as e:
            logging.error(f"An error occurred while creating file: {e}")
            raise FuseOSError(errno.EIO)  # I/O error

        return self.fd

def main():

    mountpoint = "./test7"
    bucket_name = "fuse_ecc"

    FUSE(GCSFS(bucket_name=bucket_name), mountpoint, debug=True, nothreads=True, foreground=True, allow_other=True)

if __name__ == '__main__':
    main()
