import shutil, sys, tempfile, unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from chunkfile import *

class TestChunkFileOpen(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testEmptyMode(self):
        self.assertRaises(ValueError, ChunkFile.open, self.tmpdir, mode='')

    def testNoRWA(self):
        self.assertRaises(ValueError, ChunkFile.open, self.tmpdir, mode='z')

    def testRNonexisting(self):
        self.assertRaises(IOError, ChunkFile.open, self.tmpdir.joinpath('NONEXISTING'), mode='r')

    def testRExistingGood(self):
        testdata = 'this is some data'

        # create one first
        f = ChunkFile.open(self.tmpdir, mode='w')
        f.write(testdata)
        f.close()

        f = ChunkFile.open(self.tmpdir, mode='r')
        self.assertEqual(f.read(), testdata)

    def testRExistingSubdir(self):
        sub = self.tmpdir/'subdir'
        sub.mkdir()

        self.assertRaises(IOError, ChunkFile.open, self.tmpdir, mode='r')

    def testRExistingBadChunk(self):
        p = self.tmpdir/'badchunk'
        with p.open('wb') as f:
            f.write('randomdata')

        self.assertRaises(IOError, ChunkFile.open, self.tmpdir, mode='r')

    def testRExistingRepeatedChunkNum(self):
        # create one first
        f = ChunkFile.open(self.tmpdir, mode='w')
        f.write('this is a test')
        f.close()

        # Order is important here, otherwise the glob will pick up dst and
        #   open it as src.
        with list(self.tmpdir.glob('*'))[0].open('rb') as src:
            with tempfile.NamedTemporaryFile(dir=str(self.tmpdir), delete=False) as dst:
                dst.write(src.read())

        self.assertRaises(IOError, ChunkFile.open, self.tmpdir, mode='r')

    def testWExistingEmpty(self):
        f = ChunkFile.open(self.tmpdir, mode='w')
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].stat().st_size, HEADERSIZE)

    def testWExistingTruncate(self):
        # create one first
        f = ChunkFile.open(self.tmpdir, mode='w')
        f.write('blah blah blah')
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0].stat().st_size > HEADERSIZE)

        f = ChunkFile.open(self.tmpdir, mode='w+')
        data = f.read()
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].stat().st_size, HEADERSIZE)
        self.assertEqual(data, '')

    def testRFile(self):
        path = self.tmpdir/'file'

        # create a file
        with path.open('wb') as f:
            pass

        self.assertRaises(ValueError, ChunkFile.open, path, mode='r')

    def testWFile(self):
        path = self.tmpdir/'file'

        # create a file
        with path.open('wb') as f:
            pass

        self.assertRaises(IOError, ChunkFile.open, path, mode='w')

    def testWMissingDir(self):
        path = self.tmpdir/'missing'/'dir'

        self.assertRaises(IOError, ChunkFile.open, path, mode='w')

    def testNonStringPath(self):
        self.assertRaises(TypeError, ChunkFile.open, 3, mode='w')

    def testBInMode(self):
        self.assertRaises(ValueError, ChunkFile.open, self.tmpdir, mode='wb')

    def testUInMode(self):
        self.assertRaises(NotImplementedError, ChunkFile.open, self.tmpdir, mode='U')
        self.assertRaises(NotImplementedError, ChunkFile.open, self.tmpdir, mode='rU')

    def testWRead(self):
        f = ChunkFile.open(self.tmpdir, 'w')
        self.assertRaises(IOError, f.read, 10)
        f.close()

    def testRWrite(self):
        f = ChunkFile.open(self.tmpdir, 'r')
        self.assertRaises(IOError, f.write, 'x')
        f.close()

    def testRTruncate(self):
        f = ChunkFile.open(self.tmpdir, 'r')
        self.assertRaises(IOError, f.truncate)
        f.close()

# TODO: test open with string filename works
# TODO: test open with Path filename works

if __name__ == '__main__':
    unittest.main()
