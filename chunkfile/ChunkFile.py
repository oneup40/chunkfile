import hashlib,os
from pathlib import Path

SIGNATURE = "CHNKFILE"
VERSION = (0,1)
IFACE_VERSION = 1
HEADERSIZE = 4096
CHUNKSIZE = 512 * 1024 * 1024
CHUNKDATASIZE = CHUNKSIZE - HEADERSIZE

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
    def size(): return HEADERSIZE

    def pack_into(self, buf):
        if len(buf) < self.size():
            raise ValueError('buf not large enough (need %d bytes)' % (self.size(),))

        # 00-07: CHNKFILE
        if len(self.sig) != 8:
            raise InvalidHeaderError('Sig should be 8 characters')
        buf[0x00:0x08] = '{:>8}'.format(self.sig)

        # 08-13: 000.000.000\n
        # even with a new version every week, this gives us 19.2 years
        # 83.3 years for a new version every month
        if self.version[0] < 0 or self.version[0] > 999:
            raise InvalidHeaderError('Major version should be 0-999')
        if self.version[1] < 0 or self.version[1] > 999:
            raise InvalidHeaderError('Minor version should be 0-999')
        if self.iface_version < 0 or self.iface_version > 999:
            raise InvalidHeaderError('Interface version should be 0-999')
        buf[0x08:0x14] = '{:0>3}.{:0>3}.{:0>3}\n'.format(self.version[0], self.version[1], self.iface_version)

        # 14-1F: 00000000000\n
        # 100 billion chunks of 512MiB apiece yields max volume size of 46.6 PiB
        if self.chunknum < 0 or self.chunknum > 99999999999:
            raise InvalidHeaderError('Chunknum should be 0-99999999999')
        buf[0x14:0x20] = '{:0>11}\n'.format(self.chunknum)

        # 20-2F: ...............\n
        # single char flags
        if len(self.flags) > 15:
            raise InvalidHeaderError('More than 15 flags not supported')
        buf[0x20:0x30] = '{:.<15}\n'.format(self.flags)

        # 30-3F: sha256         \n
        # hash method
        if len(self.hash_algo) > 15:
            raise InvalidHeaderError('Hash algorithm must be less than 15 chars')
        buf[0x30:0x40] = '{:<15}\n'.format(self.hash_algo)

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

        return ChunkFileHeader(sig, version, iface_version, chunknum, flags, hash_algo, hsh)

class InvalidChunkFileError(Exception): pass

class ChunkFile(object):
    def _open_existing(self, dirpath):
        entries = list(dirpath.glob('*'))
        self.chunks = [None] * len(entries)
        for entry in entries:
            if not entry.is_file():
                raise IOError('{} is not a regular file'.format(entry))

            with entry.open('rb') as f:
                data = f.read(HEADERSIZE)
                if len(data) < HEADERSIZE:
                    raise IOError('{} is not a valid chunkfile'.format(entry))

                hdr = ChunkFileHeader.unpack_from(data)
                chunknum = hdr.chunknum

                if self.chunks[chunknum]:
                    raise IOError('Multiple files with chunknum {:0>11d}'.format(chunknum))

                self.chunks[chunknum] = (hdr, entry)

    def _create_new(self, dirpath):
        if not dirpath.parent.exists():
            # same behavior as trying to open a file in a directory that doesn't exist
            raise IOError('No such file or directory: {}'.format(dirpath))

        if dirpath.exists():
            self._open_existing(dirpath)

        self.truncate(0)

    def _add_new_chunk(self):
        chunknum = len(self.chunks)
        pth = self.filename / 'chunk.{:0>11d}.dat'.format(chunknum)
        with pth.open('wb') as f:
            hsh = hashlib.sha256().hexdigest()

            hdr = ChunkFileHeader(sig=SIGNATURE, version=VERSION,
                                  iface_version=IFACE_VERSION,
                                  chunknum=len(self.chunks), flags='',
                                  hash_algo='sha256', hsh=hsh)

            buf = bytearray(HEADERSIZE)
            hdr.pack_into(buf)

            f.write(buf)

        self.chunks.append((hdr, pth))

    def _do_read(self, offset, length):
        n = offset / CHUNKDATASIZE
        if n > len(self.chunks):
            return ''

        s = ''
        with self.chunks[n][1].open('rb') as f:
            f.seek(HEADERSIZE + (offset % CHUNKDATASIZE))
            s += f.read(length)

        if len(s) < length and n+1 < len(self.chunks):
            s += self._do_read(offset + len(s), length - len(s))

        return s

    def _do_write(self, offset, data):
        n = offset / CHUNKDATASIZE
        while n >= len(self.chunks):
            self._add_new_chunk()

        nextchunkofs = (n+1) * CHUNKDATASIZE
        nbytes = nextchunkofs - offset
        with self.chunks[n][1].open('r+b') as f:
            f.seek(HEADERSIZE + (offset % CHUNKDATASIZE))
            f.write(data[:nbytes])

        if len(data) > nbytes:
            self._do_write(nextchunkofs, data[nbytes:])

    def _nbytes(self):
        nbytes = 0

        if len(self.chunks) > 1:
            nbytes += (len(self.chunks) - 1) * CHUNKDATASIZE

        if self.chunks:
            nbytes += self.chunks[-1][1].stat().st_size

        return nbytes

    def _update_hash(self, n):
        chunk = self.chunks[n]
        with chunk[1].open('r+b') as f:
            f.seek(HEADERSIZE)
            data = f.read()
            hsh = hashlib.sha256(data).hexdigest()

            chunk[0].hash_algo = 'sha256'
            chunk[0].hash = hsh

            buf = bytearray(HEADERSIZE)
            chunk[0].pack_into(buf)

            f.seek(0)
            f.write(buf)

    # public API starts here

    # If we're trying to mimic Python file functionality, here's what we need:
    #
    # __init__(name[, mode[, buffering]])
    # open(name[, mode[, buffering]]) -- staticmethod
    #   name: nothing interesting
    #   mode:
    #        one of:
    #           'r': read -- don't modify file
    #            'w': write -- truncate file while opening
    #            'a': append -- don't truncate file while opening; all writes
    #                            are at EOF regardless of current position.
    #                            Default.
    #        modifiers:
    #            'b': binary -- don't convert '\n' chars
    #            '+': allow both read and write access
    #            'U': universal newlines -- '\n', '\r', '\r\n' are all newlines
    #    buffering:
    #        0: unbuffered
    #        1: line buffered
    #        >0: apx. buffer size
    #        <0: system default
    #
    # We're not very interested in using chunkfiles for plaintext for now.
    # Accordingly, we won't support 'b' or 'U' in mode, or 1 for buffering.

    def __init__(self, dirpath, mode='a'):
        self.filename = Path(dirpath)
        self.mode = mode
        self.chunks = []
        self.closed = False
        self.offset = 0
        self.access = ''

        if not mode:
            raise ValueError('empty mode string')
        if 'U' in mode:
            raise NotImplementedError('universal newline mode')
        if mode[0] not in 'rwa':
            raise ValueError("mode string must begin with one of 'r', 'w', or 'a', not \"{}\"".format(mode))

        dirpath = Path(dirpath)
        if mode[0] == 'r':
            self.access = 'r'
            if not dirpath.exists():
                raise IOError('No such directory: {}'.format(dirpath))
            if not dirpath.is_dir():
                raise ValueError('The specified path is not a directory: {}'.format(dirpath))

            self._open_existing(dirpath)

        if mode[0] == 'w':
            self.access = 'w'
            self._create_new(dirpath)

        if mode[0] == 'a':
            raise NotImplementedError('append mode')

            # TODO
            self.access = 'w'

            if dirpath.exists():
                if not dirpath.is_dir():
                    raise ValueError('The specified path is not a directory: {}'.format(dirpath))

                self._open_existing(dirpath)
            else:
                self._create_new(dirpath)

        for c in mode[1:]:
            if c == '+':
                self.access = 'rw'
            else:
                raise ValueError("Invalid mode ('{}')".format(mode))

    # TODO: buffering
    @staticmethod
    def open(dirpath, mode='a'):
        return ChunkFile(dirpath, mode)

    # file.close(): close the file, deny further access
    def close(self):
        if not self.closed:
            self.flush()
            self.closed = True

    # file.flush(): flush the internal buffer
    def flush(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        # we don't currently buffer anything :(
        # TODO: track which chunks are dirty and only update their hashes
        for i in range(len(self.chunks)):
            self._update_hash(i)

    # file.fileno(): provide internal file descriptor. Chunkfiles do NOT
    #                     have an FD!

    # file.isatty(): Python docs say this should *not* be implemented if
    #                     there is not a real file associated.

    # file.next(): Read until the next line. We're not plaintext-focused so
    #                we don't support it.

    # file.read([size]): Read up to *size* bytes. Return less than *size*
    #                        bytes if EOF is hit. If *size* is negative or
    #                        omitted, read all data.
    def read(self, size=-1):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if 'r' not in self.access:
            raise IOError('File not open for reading')

        if size < 0:
            size = self._nbytes() - self.offset

        data = self._do_read(self.offset, size)
        self.offset += len(data)

        return data

    # file.readline([size]): Read one line. We're not plaintext-focused so
    #                            we don't support it.

    # file.readlines([sizehint]): Read all lines. We're not plaintext-focused
    #                                so we don't support it.

    # file.xreadlines(): Returns an iterator of lines in file. We're not
    #                        plaintext-focused so we don't support it.

    # file.seek(offset[, whence]): Set current position.
    #    whence: os.SEEK_SET, os.SEEK_CUR, os.SEEK_END
    #   Note that if the file was opened with 'a' mode, this only affects the
    #        read position, not the write position.
    def seek(self, offset, whence=os.SEEK_SET):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if whence == os.SEEK_SET:
            startofs = 0
        elif whence == os.SEEK_CUR:
            startofs = self.offset
        elif whence == os.SEEK_END:
            startofs = self._nbytes()
        else:
            # TODO: figure out what python usually does for this case
            startofs = 0

        self.offset = startofs + offset

    # file.tell(): Return the file's current position.
    def tell(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        return self.offset

    # file.truncate([size]): Reduce the file's size.  Current position is
    #                            not changed. Handling size > current size is
    #                            platform-dependent.
    def truncate(self, size=None):
        # TODO: figure out what default does
        # TODO: figure out what size > current size does on various platforms

        if self.closed:
            raise ValueError('I/O operation on closed file')

        if 'w' not in self.access:
            raise IOError('File not open for writing')

        nchunks = size / CHUNKDATASIZE
        if nchunks == 0:
            nchunks = 1

        for chunk in self.chunks[nchunks:]:
            chunk[1].unlink()

        self.chunks = self.chunks[:nchunks]

        while len(self.chunks) < nchunks:
            self._add_new_chunk()

        with self.chunks[-1][1].open('r+b') as f:
            f.truncate(size % CHUNKDATASIZE)

    # file.write(str): Write str to file.
    def write(self, s):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if 'w' not in self.access:
            raise IOError('File not open for writing')

        self._do_write(self.offset, s)
        self.offset += len(s)

    # file.writelines(sequence): We're not plaintext-focused so we don't
    #                                support it.

    # file.closed: boolean; read-only
    # file.encoding: We're not plaintext-focused so we don't support it.
    # file.errors: ?
    # file.mode: The mode parameter passed to open. Read-only.
    # file.name: The name parameter passed to open. Read-only.
    # file.newlines: We're not plaintext-focused so we don't support it.
    # file.softspace: We're not plaintext-focused so we don't support it.

open = ChunkFile.open
__all__ = ['SIGNATURE', 'VERSION', 'IFACE_VERSION', 'HEADERSIZE', 'CHUNKSIZE',
           'CHUNKDATASIZE', 'InvalidChunkFileError', 'ChunkFile', 'open']
