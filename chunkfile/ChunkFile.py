import os
from pathlib import Path

SIGNATURE = "CHNKFILE"
VERSION = (0,1)
IFACE_VERSION = 1

class InvalidHeaderError(Exception): pass
class UnsupportedVersionError(Exception): pass

class ChunkFileHeader(object):
    # Header page uses 4KiB of each 512MiB chunk, 0.00077% overhead

    def __init__(self, sig, version, iface_version, chunknum, flags, hash_algo, hsh):
        self.sig = sig
        self.version = version
        self.iface_version = iface_version
        self.chunknum = chunknum
        self.flags = flags
        self.hash_algo = hash_algo
        self.hash = hsh

    @staticmethod
    def size(): return 4096

    def pack_into(self, buf):
        if len(buf) < self.size():
            raise ValueError('buf not large enough (need %d bytes)' % (self.size(),))
        
        # 00-07: CHNKFILE
        if len(self.sig) != 8:
            raise InvalidHeaderError('Sig should be 8 characters')
        buf[0x00:0x08] = '{:>.8}'.format(self.sig)

        # 08-13: 000.000.000|
        # even with a new version every week, this gives us 19.2 years
        # 83.3 years for a new version every month
        if self.version[0] < 0 or self.version[0] > 999:
            raise InvalidHeaderError('Major version should be 0-999')
        if self.version[1] < 0 or self.version[1] > 999:
            raise InvalidHeaderError('Minor version should be 0-999')
        if self.iface_version < 0 or self.iface_version > 999:
            raise InvalidHeaderError('Interface version should be 0-999')
        buf[0x08:0x14] = '{:0>3}.{:0>3}.{0>3}|'.format(self.version[0], self.version[1], self.iface_version)
        
        # 14-1F: 00000000000|
        # 100 billion chunks of 512MiB apiece yields max volume size of 46.6 PiB
        if self.chunknum < 0 or self.chunknum > 99999999999:
            raise InvalidHeaderError('Chunknum should be 0-99999999999')
        buf[0x14:0x20] = '{:0>11}|'.format(self.chunknum)

        # 20-2F: ...............\n
        # single char flags
        if len(self.flags) > 15:
            raise InvalidHeaderError('More than 15 flags not supported')
        buf[0x20:0x30] = '{:.<15}\n'.format(flags)

        # 30-3F: sha256         \n
        # hash method
        if len(self.hash_algo) > 15:
            raise InvalidHeaderError('Hash algorithm must be less than 15 chars')
        buf[0x30:0x40] = '{:>.15}\n'.format(self.hash_algo)

        # 40-FF: 00000...00000\n...\n
        # hash in hex, followed by newline, followed by any ascii-printable
        #   padding until FE, followed by newline
        # max hash size is 190 hex chars which is 760 bits
        buf[0x40:0x100] = '{:<191}\n'.format(self.hash + '\n')

    @classmethod
    def unpack_from(self, buf):
        if len(buf) < self.size():
            raise ValueError('buf not large enough (need %d bytes)' % (self.size(),))
        
        sig = buf[0x00:0x08]
        verdata = buf[0x08:0x14]
        chunknumdata = buf[0x14:0x20]
        flags = buf[0x20:0x2F]
        hash_algo = buf[0x30:0x40].strip()
        hsh = buf[0x40:0x100]

        if sig != SIGNATURE:
            raise InvalidHeaderError('Bad signature, expected CHNKFILE')

        try:
            version = int(verdata[0:3]), int(verdata[4:7])
            iface_version = int(verdata[8:11])
            if version[0] < 0 or version[1] < 0 or iface_version < 0:
                raise ValueError
        except ValueError:
            raise InvalidHeaderError('Invalid version')
        if iface_version > IFACE_VERSION:
            raise UnsupportedVersionError("Can't read chunks created by version %d.%d" % version)

        try:
            chunknum = int(chunknumdata[0:11])
            if chunknum < 0: raise ValueError
        except ValueError:
            raise InvalidHeaderError('Invalid chunknum')

        return ChunkFileHeader(sig, version, chunknum, flags, hash_algo, hsh)

class InvalidChunkFileError(Exception): pass

class ChunkFile(object):
    # If we're trying to mimic Python file functionality, here's what we need:
	#
	# __init__(name[, mode[, buffering]])
    # open(name[, mode[, buffering]]) -- staticmethod
    #   name: nothing interesting
    #   mode:
	#		one of:
    #       	'r': read -- don't modify file
	#			'w': write -- truncate file while opening
	#			'a': append -- don't truncate file while opening; all writes
	#							are at EOF regardless of current position.
	#							Default.
	#		modifiers:
	#			'b': binary -- don't convert '\n' chars
	#			'+': allow both read and write access
	#			'U': universal newlines -- '\n', '\r', '\r\n' are all newlines
	#	buffering:
	#		0: unbuffered
	#		1: line buffered
	#		>0: apx. buffer size
	#		<0: system default
	#
	# We're not very interested in using chunkfiles for plaintext for now.
	# Accordingly, we won't support 'b' or 'U' in mode, or 1 for buffering.
	
	def _open_existing(self, dirpath):
		self.filename = dirpath
		self.chunks = []
		for entry in dirpath.glob('*'):
			if not entry.is_file():
				raise InvalidChunkFileError('{} is not a regular file'.format(entry))

			with entry.open('rb') as f:
				hdr = ChunkFileHeader.unpack(f)
				self.chunks.append((hdr, entry))
				
	def _create_new(self, dirpath):
		self.filename = dirpath
		self.chunks = []
		if not dirpath.parent.exists():
			# same behavior as trying to open a file in a directory that doesn't exist
			raise IOError('No such file or directory: {}'.format(dirpath))
				
	# TODO: buffering
	@staticmethod
	def open(dirpath, mode='a'):
		if not mode:
			raise ValueError('empty mode string')
		if mode[0] not in 'rwa':
			raise ValueError("mode string must begin with one of 'r', 'w', or 'a', not \"{}\"".format(mode))
			
		dirpath = Path(dirpath)
		if mode[0] == 'r':
			if not dirpath.exists():
				raise IOError('No such directory: {}'.format(dirpath))
			if not dirpath.is_dir():
				raise ValueError('The specified path is not a directory: {}'.format(dirpath))
				
			self._open_existing(dirpath)
			
		if mode[0] == 'w':
			self._create_new(dirpath)
			
		if mode[0] == 'a':
			if dirpath.exists():
				if not dirpath.is_dir():
					raise ValueError('The specified path is not a directory: {}'.format(dirpath))
					
				self._open_existing(dirpath)
			else:
				self._create_new(dirpath)
	#
	# file.close(): close the file, deny further access
	# file.flush(): flush the internal buffer
	# file.fileno(): provide internal file descriptor. Chunkfiles do NOT
	# 					have an FD!
	# file.isatty(): Python docs say this should *not* be implemented if
	# 					there is not a real file associated.
	# file.next(): Read until the next line. We're not plaintext-focused so
	#				we don't support it.
	# file.read([size]): Read up to *size* bytes. Return less than *size*
	#						bytes if EOF is hit. If *size* is negative or
	#						omitted, read all data.
	# file.readline([size]): Read one line. We're not plaintext-focused so
	#							we don't support it.
	# file.readlines([sizehint]): Read all lines. We're not plaintext-focused
	#								so we don't support it.
	# file.xreadlines(): Returns an iterator of lines in file. We're not 
	#						plaintext-focused so we don't support it.
	# file.seek(offset[, whence]): Set current position.
	#	whence: os.SEEK_SET, os.SEEK_CUR, os.SEEK_END
	#   Note that if the file was opened with 'a' mode, this only affects the
	#		read position, not the write position.
	# file.tell(): Return the file's current position.
	# file.truncate([size]): Reduce the file's size.  Current position is
	#							not changed. Handling size > current size is
	#							platform-dependent.
	# file.write(str): Write str to file.
	# file.writelines(sequence): We're not plaintext-focused so we don't
	#								support it.
	#
	# file.closed: boolean; read-only
	# file.encoding: We're not plaintext-focused so we don't support it.
	# file.errors: ?
	# file.mode: The mode parameter passed to open. Read-only.
	# file.name: The name parameter passed to open. Read-only.
	# file.newlines: We're not plaintext-focused so we don't support it.
	# file.softspace: We're not plaintext-focused so we don't support it.
	
open = ChunkFile.open