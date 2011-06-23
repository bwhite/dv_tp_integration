"""Make fake video frames

Usage:
python video_frame_generator.py <hdfs_out_path>
"""
import hadoopy
import random
import string
import sys


def rand_string(l):
    return ''.join(random.choice(string.letters) for i in xrange(l))

# Format: ('image_hash', {'image_data': 'longbinarydata', 'frame_num': 5, 'source_video': 'long_hash'})
def generate():
    def _inner():
        image_data = rand_string(100)
        frame_num = random.randint(0, 2**10)
        source_video = rand_string(32)
        return rand_string(32), locals()
    for x in range(50):
        yield _inner()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    hadoopy.writetb(sys.argv[1], generate())
