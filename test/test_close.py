import shutil, sys, tempfile, unittest
from pathlib import Path

#sys.path.append(str(Path(__file__).parent.parent))
from chunkfile import *

class TestChunkFileClose(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))

    def testDoubleClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()
        f.close()

    def testFlushAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.flush)

    def testReadAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.read, 3)

    def testSeekAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.seek, 0)

    def testTellAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.tell)

    def testTruncateAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.truncate)

    def testWriteAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.write, 'xyz'.encode('ascii'))


if __name__ == '__main__':
    unittest.main()
