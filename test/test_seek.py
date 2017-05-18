import os, shutil, sys, tempfile, unittest
from pathlib import Path

from chunkfile import *

class TestChunkFileSeek(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testDefaultSeekMode(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.seek(512)
        self.assertEqual(f.tell(), 512)
        f.seek(512)
        self.assertEqual(f.tell(), 512)

    def testSeekSet(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.seek(512, os.SEEK_SET)
        self.assertEqual(f.tell(), 512)
        f.seek(128, os.SEEK_SET)
        self.assertEqual(f.tell(), 128)

    def testSeekSetNegative(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        self.assertRaises(IOError, f.seek, -1, os.SEEK_SET)

    def testSeekCur(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.seek(100, os.SEEK_CUR)
        self.assertEqual(f.tell(), 100)
        f.seek(22, os.SEEK_CUR)
        self.assertEqual(f.tell(), 122)
        f.seek(-10, os.SEEK_CUR)
        self.assertEqual(f.tell(), 112)

    def testSeekCurNegative(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.seek(20)
        self.assertRaises(IOError, f.seek, -21, os.SEEK_CUR)

    def testSeekEnd(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.write(b'x' * 1024)
        f.close()

        f = ChunkFile.open(self.tmpdir, 'rb')
        f.seek(0, os.SEEK_END)
        self.assertEqual(f.tell(), 1024)
        f.seek(-100, os.SEEK_END)
        self.assertEqual(f.tell(), 924)

    def testSeekEndNegative(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.write(b'x' * 64)
        f.close()

        f = ChunkFile.open(self.tmpdir, 'rb')
        self.assertRaises(IOError, f.seek, -100, os.SEEK_END)

    def testHugeOffset(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        self.assertRaises(IOError, f.seek, 2**65)

    def testSeekClosed(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()
        self.assertRaises(ValueError, f.seek, 1)

    def testSeekInvalidMode(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        self.assertRaises(IOError, f.seek, 0, 2348203)

if __name__ == '__main__':
    unittest.main()
