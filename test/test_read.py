import shutil, sys, tempfile, unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from chunkfile import *

class TestChunkFileRead(unittest.TestCase):
    def setUp(self):
        self.testdata = 'abcdefghijklmnopqrstuvwxyz'.encode('ascii')
        self.tmpdir = Path(tempfile.mkdtemp())

        f = ChunkFile.open(self.tmpdir, 'wb')
        f.write(self.testdata)
        f.close()

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testRead(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read(len(self.testdata))
        self.assertEqual(data, self.testdata)
        self.assertEqual(f.tell(), len(self.testdata))
        f.close()

    def testReadDefault(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read()
        self.assertEqual(data, self.testdata)
        self.assertEqual(f.tell(), len(self.testdata))
        f.close()

    def testReadNegative(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read(-57954)
        self.assertEqual(data, self.testdata)
        self.assertEqual(f.tell(), len(self.testdata))
        f.close()

    def testReadClosed(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        f.close()

        self.assertRaises(ValueError, f.read)

    def testReadZero(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read(0)
        self.assertEqual(data, b'')
        self.assertEqual(f.tell(), 0)
        f.close()

    def testReadLong(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read(len(self.testdata) * 100)
        self.assertEqual(data, self.testdata)
        self.assertEqual(f.tell(), len(self.testdata))
        f.close()

    def testReadW(self):
        tmpdir2 = Path(tempfile.mkdtemp())

        try:
            f = ChunkFile.open(tmpdir2, 'wb')
            self.assertRaises(IOError, f.read)
        finally:
            shutil.rmtree(str(tmpdir2))

    def testReadCrossChunk(self):
        tmpdir2 = Path(tempfile.mkdtemp())

        try:
            f = ChunkFile.open(tmpdir2, 'wb')
            f.write('a'.encode('ascii') * CHUNKDATASIZE)
            f.write('b'.encode('ascii') * CHUNKDATASIZE)
            f.close()

            f = ChunkFile.open(tmpdir2, 'rb')
            f.seek(CHUNKDATASIZE - 10)
            data = f.read(20)
            self.assertEqual(data, b'a'*10 + b'b'*10)
            self.assertEqual(f.tell(), CHUNKDATASIZE + 10)
        finally:
            shutil.rmtree(str(tmpdir2))

    def testReadHugeOffset(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        ofs = 20 * CHUNKSIZE

        f.seek(ofs)

        # standard behavior is to not move offset after read
        #   if current pos is past EOF
        data = f.read(5)
        self.assertEqual(data, b'')
        self.assertEqual(f.tell(), ofs)

        data = f.read()
        self.assertEqual(data, b'')
        self.assertEqual(f.tell(), ofs)
        
    def testReadAllCrossChunk(self):
        tmpdir2 = Path(tempfile.mkdtemp())

        try:
            f = ChunkFile.open(tmpdir2, 'wb')
            f.write('a'.encode('ascii') * CHUNKDATASIZE)
            f.write('b'.encode('ascii') * 10)
            f.close()

            f = ChunkFile.open(tmpdir2, 'rb')
            f.seek(CHUNKDATASIZE - 10)
            data = f.read()
            self.assertEqual(data, b'a'*10 + b'b'*10)
            self.assertEqual(f.tell(), CHUNKDATASIZE + 10)
        finally:
            shutil.rmtree(str(tmpdir2))



if __name__ == '__main__':
    unittest.main()
