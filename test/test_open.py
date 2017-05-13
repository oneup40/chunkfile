import shutil, tempfile, unittest
from pathlib import Path
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
			
if __name__ == '__main__':
	unittest.main()