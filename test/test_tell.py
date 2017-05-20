import os, shutil, sys, tempfile, unittest
from pathlib import Path

from chunkfile import *

class TestChunkFileTell(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(str(self.tmpdir))


    def testTellMultipleChunk(self):
        f = ChunkFile.open(self.tmpdir, 'wb')

        x1M = b'x' * 1024 * 1024
        written = 0

        while written < CHUNKDATASIZE * 3:
            f.write(x1M)
            written += len(x1M)
            self.assertEqual(f.tell(), written)

if __name__ == '__main__':
    unittest.main()
