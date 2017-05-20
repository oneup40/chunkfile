import os, shutil, sys, tempfile, unittest
from pathlib import Path

from chunkfile import *

class TestChunkFileTruncate(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testTruncateAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.truncate, 0)

    def testTruncateReadonly(self):
        f = ChunkFile.open(self.tmpdir, 'rb')

        self.assertRaises(IOError, f.truncate, 0)

    def testTruncateMultipleChunks(self):
        f = ChunkFile.open(self.tmpdir, 'wb')

        x1M = b'x' * 1024 * 1024
        written = 0
        while written < CHUNKDATASIZE * 3:
            f.write(x1M)
            written += len(x1M)

        self.assertEqual(f.tell(), written)
        f.truncate(CHUNKDATASIZE + 100)
        self.assertEqual(f.tell(), written)
        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 2)

        totalsize = sum([x.stat().st_size for x in filelist])
        self.assertEqual(totalsize, HEADERSIZE * 2 + CHUNKDATASIZE + 100)

    def testTruncateChunkBoundary(self):
        f = ChunkFile.open(self.tmpdir, 'wb')

        x1M = b'x' * 1024 * 1024
        written = 0
        while written < CHUNKDATASIZE * 3:
            f.write(x1M)
            written += len(x1M)

        self.assertEqual(f.tell(), written)
        f.truncate(CHUNKDATASIZE * 2)
        self.assertEqual(f.tell(), written)
        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 2)

        totalsize = sum([x.stat().st_size for x in filelist])
        self.assertEqual(totalsize, HEADERSIZE * 2 + CHUNKDATASIZE * 2)

    def testWriteAfterTruncate(self):
        # truncate does *NOT* move the current file position!
        # $ python2
        # >>> f = open('/tmp/foo', 'wb')
        # >>> f.write('x' * 4096)
        # >>> f.tell()
        # 4096
        # >>> f.truncate(128)
        # >>> f.tell()
        # 4096
        # >>> f.write('x')
        # >>> f.tell()
        # 4097
        # >>> exit()
        # $ ls -l /tmp/foo
        # -rw-rw-rw- 1 user user 4097 Jan 1 1900 /tmp/foo

        f = ChunkFile.open(self.tmpdir, 'wb')

        f.write(b'x' * 4096)
        self.assertEqual(f.tell(), 4096)

        f.truncate(128)
        self.assertEqual(f.tell(), 4096)

        f.write(b'x')
        self.assertEqual(f.tell(), 4097)

        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 1)
        self.assertEqual(filelist[0].stat().st_size, HEADERSIZE + 4097)

        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read()
        self.assertEqual(data, b'x' * 128 + b'\x00' * (4096 - 128) + b'x')

    def testTruncateZeroFill(self):
        # On Linux at least, truncate past EOF extends the file.
        # $ python2
        # >>> import os
        # >>> f = open('/tmp/x','wb')
        # >>> f.truncate(4096)
        # >>> f.tell()
        # 0
        # >>> f.seek(0, os.SEEK_END)
        # >>> f.tell()
        # 4096
        # >>> exit()
        # $ ls -l /tmp/x
        # -rw-rw-rw- 1 user user 4096 Jan 1 1900 /tmp/x

        f = ChunkFile.open(self.tmpdir, 'wb')
        f.truncate(4096)
        self.assertEqual(f.tell(), 0)
        f.seek(0, os.SEEK_END)
        self.assertEqual(f.tell(), 4096)
        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 1)
        self.assertEqual(filelist[0].stat().st_size, HEADERSIZE + 4096)

        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read()
        self.assertEqual(data, b'\x00' * 4096)

    def testTruncateZeroFillMultipleChunks(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.truncate(CHUNKDATASIZE * 3 + 100)
        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 4)
        totalsize = sum([pth.stat().st_size for pth in filelist])
        self.assertEqual(totalsize, HEADERSIZE * 4 + CHUNKDATASIZE * 3 + 100)

        f = ChunkFile.open(self.tmpdir, 'rb')
        read = 0
        while read < CHUNKDATASIZE * 3 + 100:
            data = f.read(1024 * 1024)
            self.assertEqual(data, b'\x00' * len(data))
            read += len(data)

if __name__ == '__main__':
    unittest.main()
