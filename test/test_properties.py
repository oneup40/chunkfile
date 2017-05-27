import shutil, sys, tempfile, unittest
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from chunkfile import *

class TestChunkFileRead(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testGetClosed(self):
        f = ChunkFile.open(self.tmpdir)
        self.assertEqual(f.closed, False)

        f.close()
        self.assertEqual(f.closed, True)

    def testSetClosed(self):
        f = ChunkFile.open(self.tmpdir)
        def try_set_closed():
            f.closed = False
        self.assertRaises(AttributeError, try_set_closed)

    def testGetMode(self):
        f1 = ChunkFile.open(self.tmpdir, 'ab')
        self.assertEqual(f1.mode, 'ab')
        f1.close()

        f2 = ChunkFile.open(self.tmpdir, 'w+b')
        self.assertEqual(f2.mode, 'w+b')
        f2.close()

    def testSetMode(self):
        f = ChunkFile.open(self.tmpdir)
        def try_set_mode():
            f.mode = 'x'
        self.assertRaises(AttributeError, try_set_mode)

    def testGetName(self):
        f = ChunkFile.open(self.tmpdir)
        self.assertEqual(f.name, str(self.tmpdir))

    def testSetName(self):
        f = ChunkFile.open(self.tmpdir)
        def try_set_name():
            f.name = str(self.tmpdir / "foobar")
        self.assertRaises(AttributeError, try_set_name)

if __name__ == '__main__':
    unittest.main()
