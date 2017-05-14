import shutil, sys, tempfile, unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
import chunkfile

class TestChunkFileOpen(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testEmptyMode(self):
        with self.assertRaises(ValueError):
            chunkfile.ChunkFile.open(self.tmpdir, mode='')

    def testNoRWA(self):
        with self.assertRaises(ValueError):
            chunkfile.ChunkFile.open(self.tmpdir, mode='z')

    def testRNonexisting(self):
        with self.assertRaises(IOError):
            chunkfile.ChunkFile.open(self.tmpdir.joinpath('NONEXISTING'), mode='r')

    def testWExisting(self):
        f = chunkfile.ChunkFile.open(self.tmpdir, mode='w')
        f.close()

        entries = list(self.tmpdir.glob('*'))
        self.assertEquals(len(entries), 1)
        self.assertEquals(entries[0].stat().st_size, chunkfile.HEADERSIZE)

# TODO: test open with string filename works
# TODO: test open with Path filename works

if __name__ == '__main__':
    unittest.main()
