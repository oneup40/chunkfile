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
        self.assertRaises(ValueError, ChunkFile.open, self.tmpdir, mode='zb')

    def testBadModeFlag(self):
        self.assertRaises(ValueError, ChunkFile.open, self.tmpdir, mode='rzb')

    def testNoB(self):
        self.assertRaises(NotImplementedError, ChunkFile.open, self.tmpdir, mode='r')

    def testRNonexisting(self):
        self.assertRaises(IOError, ChunkFile.open, self.tmpdir.joinpath('NONEXISTING'), mode='rb')

    def testRExistingGood(self):
        testdata = 'this is some data'.encode('ascii')

        # create one first
        f = ChunkFile.open(self.tmpdir, mode='wb')
        f.write(testdata)
        f.close()

        f = ChunkFile.open(self.tmpdir, mode='rb')
        self.assertEqual(f.read(), testdata)

    def testRExistingSubdir(self):
        sub = self.tmpdir/'subdir'
        sub.mkdir()

        self.assertRaises(IOError, ChunkFile.open, self.tmpdir, mode='rb')

    def testRExistingBadChunk(self):
        p = self.tmpdir/'badchunk'
        with p.open('wb') as f:
            f.write('randomdata'.encode('ascii'))

        self.assertRaises(IOError, ChunkFile.open, self.tmpdir, mode='rb')

    def testRExistingRepeatedChunkNum(self):
        # create one first
        f = ChunkFile.open(self.tmpdir, mode='wb')
        f.write('this is a test'.encode('ascii'))
        f.close()

        # Order is important here, otherwise the glob will pick up dst and
        #   open it as src.
        with list(self.tmpdir.glob('*'))[0].open('rb') as src:
            with tempfile.NamedTemporaryFile(dir=str(self.tmpdir), delete=False) as dst:
                dst.write(src.read())

        self.assertRaises(IOError, ChunkFile.open, self.tmpdir, mode='rb')

    def testWNonExisting(self):
        pth = self.tmpdir/'newdir'
        self.assertTrue(not pth.exists())

        f = ChunkFile.open(pth, mode='wb')
        f.close()

        self.assertTrue(pth.exists())

        filelist = list(pth.glob('*'))
        self.assertEqual(len(filelist), 0)

    def testWExistingEmpty(self):
        f = ChunkFile.open(self.tmpdir, mode='wb')
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 0)

    def testWExistingTruncate(self):
        # create one first
        f = ChunkFile.open(self.tmpdir, mode='wb')
        f.write('blah blah blah'.encode('ascii'))
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0].stat().st_size > HEADERSIZE)

        f = ChunkFile.open(self.tmpdir, mode='w+b')
        data = f.read()
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 0)
        self.assertEqual(data, b'')

    def testRFile(self):
        path = self.tmpdir/'file'

        # create a file
        with path.open('wb') as f:
            pass

        self.assertRaises(ValueError, ChunkFile.open, path, mode='rb')

    def testWFile(self):
        path = self.tmpdir/'file'

        # create a file
        with path.open('wb') as f:
            pass

        self.assertRaises(ValueError, ChunkFile.open, path, mode='wb')

    def testWMissingDir(self):
        path = self.tmpdir/'missing'/'dir'

        self.assertRaises(IOError, ChunkFile.open, path, mode='wb')

    def testNonStringPath(self):
        self.assertRaises(TypeError, ChunkFile.open, 3, mode='wb')

    def testUInMode(self):
        self.assertRaises(NotImplementedError, ChunkFile.open, self.tmpdir, mode='U')
        self.assertRaises(NotImplementedError, ChunkFile.open, self.tmpdir, mode='rU')

    def testWRead(self):
        f = ChunkFile.open(self.tmpdir, 'wb')
        self.assertRaises(IOError, f.read, 10)
        f.close()

    def testRWrite(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        self.assertRaises(IOError, f.write, 'x'.encode('ascii'))
        f.close()

    def testRTruncate(self):
        f = ChunkFile.open(self.tmpdir, 'rb')
        self.assertRaises(IOError, f.truncate)
        f.close()

    def testANonExisting(self):
        pth = self.tmpdir/'newdir'
        self.assertTrue(not pth.exists())

        f = ChunkFile.open(pth, mode='ab')
        f.close()

        self.assertTrue(pth.exists())

        filelist = list(pth.glob('*'))
        self.assertEqual(len(filelist), 0)

    def testAExistingEmpty(self):
        f = ChunkFile.open(self.tmpdir, mode='ab')
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 0)

    def testAExisting(self):
        # create one first
        f = ChunkFile.open(self.tmpdir, mode='ab')
        f.write('blah blah blah'.encode('ascii'))
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0].stat().st_size > HEADERSIZE)

        f = ChunkFile.open(self.tmpdir, mode='ab')
        data = f.read()
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0].stat().st_size > HEADERSIZE)
        self.assertEqual(data, b'blah blah blah')

    def testDefaultA(self):
        f = ChunkFile.open(self.tmpdir)
        self.assertTrue('a' in f.mode)

# TODO: test open with string filename works
# TODO: test open with Path filename works

if __name__ == '__main__':
    unittest.main()
