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
        with self.assertRaises(ValueError):
            ChunkFile.open(self.tmpdir, mode='')

    def testNoRWA(self):
        with self.assertRaises(ValueError):
            ChunkFile.open(self.tmpdir, mode='z')

    def testRNonexisting(self):
        with self.assertRaises(IOError):
            ChunkFile.open(self.tmpdir.joinpath('NONEXISTING'), mode='r')

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

        with self.assertRaises(IOError):
            ChunkFile.open(self.tmpdir, mode='r')

    def testRExistingBadChunk(self):
        p = self.tmpdir/'badchunk'
        with p.open('wb') as f:
            f.write('randomdata')

        with self.assertRaises(IOError):
            ChunkFile.open(self.tmpdir, mode='r')

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

        with self.assertRaises(IOError):
            ChunkFile.open(self.tmpdir, mode='r')

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
        self.assertGreater(entries[0].stat().st_size, HEADERSIZE)

        f = ChunkFile.open(self.tmpdir, mode='w')
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

        with self.assertRaises(ValueError):
            ChunkFile.open(path, 'r')

    def testWFile(self):
        path = self.tmpdir/'file'

        # create a file
        with path.open('wb') as f:
            pass

        with self.assertRaises(IOError):
            ChunkFile.open(path, 'w')

    def testWMissingDir(self):
        path = self.tmpdir/'missing'/'dir'

        with self.assertRaises(IOError):
            ChunkFile.open(path, 'w')


# TODO: test open with string filename works
# TODO: test open with Path filename works

if __name__ == '__main__':
    unittest.main()
