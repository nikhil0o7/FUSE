import argparse
from GCSFS import GCSFS  # Make sure to import your class

def main():
    parser = argparse.ArgumentParser(description='GCSFS command line interface.')
    parser.add_argument('command', choices=['mkdir', 'rmdir','create','open','close','opendir','readdir','mount','write', 'read', 'rename', 'unlink', 'truncate'])
    parser.add_argument('bucket_name', help='Name of the GCS bucket')
    parser.add_argument('--mode', type=lambda x: int(x, 0))
    parser.add_argument('path', help='Path to the file or directory')
    parser.add_argument('--data', help='Data to write', default=None)
    parser.add_argument('--new_name', help='New name for rename', default=None)
    parser.add_argument('--length', type=int, help='Length for truncate', default=None)

    args = parser.parse_args()

    # Initialize your filesystem
    fs = GCSFS(args.bucket_name)

    # Call the appropriate method
    if args.command == 'mkdir':
        fs.mkdir(args.path, mode=0o755)  # You need to specify the mode
    elif args.command == 'mount':
        fs.mount(args.bucket_name,args.path)
    elif args.command == 'rmdir':
        fs.rmdir(args.path)
    elif args.command == 'opendir':
        fs.opendir(args.path)
    elif args.command == 'readdir':
        fs.readdir(args.path)
    elif args.command == 'create':
        fs.create(args.path, args.mode)
    elif args.command =='open':
        fs.open(args.path, args.mode)
    elif args.command =='close':
        fs.close(args.path)
    elif args.command == 'write':
        if args.data:
            fh = fs.open(args.path, flags=0)
            fs.write(args.data.encode(), fh)  # Data needs to be bytes
            fs.close(fh)
    elif args.command == 'read':
        fh = fs.open(args.path, flags=0)
        print(fs.read(1024, fh).decode())  # Assuming you want to read 1024 bytes and output as string
        fs.close(fh)
    elif args.command == 'rename':
        if args.new_name:
            fs.rename(args.path, args.new_name)
    elif args.command == 'unlink':
        fs.unlink(args.path)
    elif args.command == 'truncate':
        if args.length is not None:
            fs.truncate(args.path, args.length)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()

