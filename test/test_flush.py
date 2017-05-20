import shutil, sys, tempfile, unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from chunkfile import *

class TestChunkFileFlush(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testFlush(self):
        f = ChunkFile.open(self.tmpdir, 'wb')

        f.flush()
        files = list(self.tmpdir.glob('*'))
        self.assertEqual(len(files), 0)

        testdata = 'asdfghjklqwertyuiopzxcvnm,'.encode('ascii')
        f.write(testdata)
        f.flush()
        files = list(self.tmpdir.glob('*'))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].stat().st_size, HEADERSIZE + len(testdata))

        f.close()
        files = list(self.tmpdir.glob('*'))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].stat().st_size, HEADERSIZE + len(testdata))

    def testFlushAfterClose(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        f.close()

        self.assertRaises(ValueError, f.flush)

if __name__ == '__main__':
    unittest.main()
