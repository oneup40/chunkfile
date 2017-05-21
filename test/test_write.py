import os, shutil, sys, tempfile, unittest
from pathlib import Path

from chunkfile import *

class TestChunkFileWrite(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testWriteAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.write, b'xyz')

    def testWriteReadonly(self):
        f = ChunkFile.open(self.tmpdir, 'rb')

        self.assertRaises(IOError, f.write, b'xyz')

    def testWriteMultipleChunks(self):
        f = ChunkFile.open(self.tmpdir, 'wb')

        data = b'x' * (CHUNKDATASIZE * 2 + 100)
        f.write(data)

        self.assertEqual(f.tell(), len(data))

        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 3)

        totalsize = sum([x.stat().st_size for x in filelist])
        self.assertEqual(totalsize, HEADERSIZE * 3 + CHUNKDATASIZE * 2 + 100)

    def testWriteChunkBoundary(self):
        f = ChunkFile.open(self.tmpdir, 'wb')

        data = b'x' * (CHUNKDATASIZE * 2)
        f.write(data)

        self.assertEqual(f.tell(), len(data))

        f.close()

        filelist = list(self.tmpdir.glob('*'))
        self.assertEqual(len(filelist), 2)

        totalsize = sum([x.stat().st_size for x in filelist])
        self.assertEqual(totalsize, CHUNKSIZE * 2)

    def testAppend(self):
        f = ChunkFile.open(self.tmpdir, 'ab')

        self.assertEqual(f.tell(), 0)
        data = b'have some data'
        f.write(data)
        self.assertEqual(f.tell(), len(data))

        f.close()

        f = ChunkFile.open(self.tmpdir, 'ab')

        self.assertEqual(f.tell(), 0)
        moredata = b'more data'
        f.write(moredata)
        self.assertEqual(f.tell(), len(data + moredata))

        f.close()

        f = ChunkFile.open(self.tmpdir, 'ab')
        x = f.read()
        self.assertEqual(x, data + moredata)

if __name__ == '__main__':
    unittest.main()
