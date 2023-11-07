# File Systems Using Cloud Storage

This program has been designed to bridge the gap between Google Cloud Storage (GCS), an object storage service, and the traditional file system interface. It achieves this by leveraging the capabilities of FUSE (Filesystem in Userspace) through the `fusepy` library, allowing the program to expose GCS buckets as if they were mounted filesystems on the host machine.

At the core is the `GCSFS` class which inherits from `fusepy`'s `Operations`. This class serves as the filesystem implementation, with methods that correspond to file operations. These methods internally call the GCS APIs to perform actions on the cloud storage. The GCS client is initialized in the constructor, and a reference to the specific GCS bucket is stored for future operations.

The `mount` method within `GCSFS` is particularly interesting as it enables the GCS bucket to be mounted onto a local directory, utilizing the `gcsfuse` command line tool. This creates a seamless integration point where users can interact with cloud storage using familiar file system semantics.

File operations such as `create`, `read`, `write`, `truncate`, `unlink`, and `rename` translate the file manipulation commands to their GCS counterparts. Since GCS does not support in-place modifications, the `write` method incorporates a read-modify-write sequence for updates. Similarly, directory-related methods like `mkdir`, `opendir`, `readdir`, and `rmdir` are designed to mimic directory behaviors in GCS’s flat namespace by manipulating object prefixes and metadata.

It also includes a rudimentary error handling mechanism that raises `FuseOSError` exceptions in response to issues, ensuring that errors are communicated back through FUSE to the user in a meaningful way. This integration not only facilitates the interaction with GCS through conventional file operations but also abstracts away the complexities of GCS for end-users. It serves as an intermediate layer, translating POSIX-like file operations to GCS API calls, and thus provides a more intuitive and productive way of working with cloud storage for applications expecting a filesystem interface.
