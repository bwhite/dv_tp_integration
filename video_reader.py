"""Read fake videoss

Usage:
python video_reader.py <hdfs_in_path>
"""
import hadoopy
import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    print('Reading[%s]' % sys.argv[1])
    for video_hash, metadata in hadoopy.readtb(sys.argv[1]):
        print('VideoData[%s] VideoHash[%s]' % (metadata['video_data'], video_hash))
