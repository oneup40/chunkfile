import shutil, sys, tempfile, unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from chunkfile import *

class TestChunkFileRead(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testContextManagerNormal(self):
        testdata = b'this is some test data'

        with ChunkFile.open(self.tmpdir, 'ab') as f:
            self.assertEqual(f.closed, False)
            self.assertEqual(f.mode, 'ab')

            f.write(testdata)

        self.assertEqual(f.closed, True)

        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read()
        self.assertEqual(data, testdata)

    def testContextManagerException(self):
        testdata = b'this is some test data'

        try:
            with ChunkFile.open(self.tmpdir, 'ab') as f:
                self.assertEqual(f.closed, False)
                self.assertEqual(f.mode, 'ab')

                f.write(testdata)

                x = 1/0
        except ZeroDivisionError:
            pass

        self.assertEqual(f.closed, True)

        f = ChunkFile.open(self.tmpdir, 'rb')
        data = f.read()
        self.assertEqual(data, testdata)

if __name__ == '__main__':
    unittest.main()
