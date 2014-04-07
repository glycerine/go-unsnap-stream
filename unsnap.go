package unsnap

// copyright (c) 2013-2014, Jason E. Aten.
// License: MIT.

// For reference, includes code at the bottom of the file from the python
// implementation of decoding the snappy framing format. See file:
// .../anaconda/install/lib/python2.7/site-packages/snappy.py
// The python code is copyright and licensed as:
/*
#
# Copyright (c) 2011, Andres Moreira <andres@andresmoreira.com>
#               2011, Felipe Cruz <felipecruz@loogica.net>
#               2012, JT Olds <jt@spacemonkey.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the authors nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL ANDRES MOREIRA BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"hash/crc32"
	"code.google.com/p/snappy-go/snappy"
)

// SnappyFile: create a drop-in-replacement/wrapper for an *os.File that handles doing the unsnappification online as more is read from it

type SnappyFile struct {
	Fname  string
	Filep  *os.File
	EncBuf FixedSizeRingBuf // holds any extra that isn't yet returned, encoded
	DecBuf FixedSizeRingBuf // holds any extra that isn't yet returned, decoded
}

var total int

// for debugging, show state of buffers
func (f *SnappyFile) Dump() {
	fmt.Printf("EncBuf has length %d and contents:\n%s\n", len(f.EncBuf.Bytes()), string(f.EncBuf.Bytes()))
	fmt.Printf("DecBuf has length %d and contents:\n%s\n", len(f.DecBuf.Bytes()), string(f.DecBuf.Bytes()))
}

func (f *SnappyFile) Read(p []byte) (n int, err error) {

	// before we unencrypt more, try to drain the DecBuf first
	n, _ = f.DecBuf.Read(p)
	if n > 0 {
		total += n
		return n, nil
	}

	//nEncRead, nDecAdded, err := UnsnapOneFrame(f.Filep, &f.EncBuf, &f.DecBuf, f.Fname)
	_, _, err = UnsnapOneFrame(f.Filep, &f.EncBuf, &f.DecBuf, f.Fname)
	if err != nil && err != io.EOF {
		panic(err)
	}

	n, _ = f.DecBuf.Read(p)

	if n > 0 {
		total += n
		return n, nil
	}
	if len(f.DecBuf.Bytes()) == 0 {
		if len(f.DecBuf.Bytes()) == 0 && len(f.EncBuf.Bytes()) == 0 {
			// only now (when EncBuf is empty) can we give io.EOF.
			// Any earlier, and we leave stuff un-decoded!
			return 0, io.EOF
		}
	}
	return 0, nil
}

func Open(name string) (file *SnappyFile, err error) {
	fp, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	snap := &SnappyFile{
		Fname:  name,
		Filep:  fp,
		EncBuf: *NewFixedSizeRingBuf(65536),     // buffer of snappy encoded bytes
		DecBuf: *NewFixedSizeRingBuf(65536 * 2), // buffer of snapppy decoded bytes
	}
	return snap, nil
}

func (f *SnappyFile) Close() error {
	return f.Filep.Close()
}

// for an increment of a frame at a time:
// read from r into encBuf (encBuf is still encoded, thus the name), and write unsnappified frames into outDecodedBuf
//  the returned n: number of bytes read from the encrypted encBuf
func UnsnapOneFrame(r io.Reader, encBuf *FixedSizeRingBuf, outDecodedBuf *FixedSizeRingBuf, fname string) (nEnc int64, nDec int64, err error) {

	nEnc = 0
	nDec = 0

	// read up to 65536 bytes from r into encBuf, at least a snappy frame
	nread, err := io.CopyN(encBuf, r, 65536) // returns nwrotebytes, err
	nEnc += nread
	if err != nil {
		if err == io.EOF {
			if nread == 0 {
				if encBuf.Readable == 0 {
					return nEnc, nDec, io.EOF
				}
				// else we have bytes in encBuf, so decode them!
				err = nil
			} else {
				// continue below, processing the nread bytes
				err = nil
			}
		} else {
			panic(err)
		}
	}

	// flag for printing chunk size alignment messages
	verbose := false

	const snappyStreamHeaderSz = 10
	const headerSz = 4
	const crc32Sz = 4
	// the magic 18 bytes accounts for the snappy streaming header and the first chunks size and checksum
	// http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt

	chunk := (*encBuf).Bytes()

	// however we exit, advance as
	//	defer func() { (*encBuf).Next(N) }()

	// 65536 is the max size of a snappy framed chunk. See
	// http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt:91
	// buf := make([]byte, 65536)

	//	fmt.Printf("read from file, b is len:%d with value: %#v\n", len(b), b)
	//	fmt.Printf("read from file, bcut is len:%d with value: %#v\n", len(bcut), bcut)

	//fmt.Printf("raw bytes of chunksz are: %v\n", b[11:14])

	fourbytes := make([]byte, 4)
	chunkCount := 0

	for nDec < 65536 {
		if len(chunk) == 0 {
			break
		}
		chunkCount++
		fourbytes[3] = 0
		copy(fourbytes, chunk[1:4])
		chunksz := binary.LittleEndian.Uint32(fourbytes)
		chunk_type := chunk[0]

		switch true {
		case chunk_type == 0xff:
			{ // stream identifier

				streamHeader := chunk[:snappyStreamHeaderSz]
				if 0 != bytes.Compare(streamHeader, []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}) {
					panic("file had chunk starting with 0xff but then no magic snappy streaming protocol bytes, aborting.")
				} else {
					//fmt.Printf("got streaming snappy magic header just fine.\n")
				}
				chunk = chunk[snappyStreamHeaderSz:]
				(*encBuf).Advance(snappyStreamHeaderSz)
				nEnc += snappyStreamHeaderSz
				continue
			}
		case chunk_type == 0x00:
			{ // compressed data
				if verbose {
					fmt.Fprintf(os.Stderr, "chunksz is %d  while  total bytes avail are: %d\n", int(chunksz), len(chunk)-4)
				}

				//crc := binary.LittleEndian.Uint32(chunk[headerSz:(headerSz + crc32Sz)])
				section := chunk[(headerSz + crc32Sz):(headerSz + chunksz)]

				dec, ok := snappy.Decode(nil, section)
				if ok != nil {
					// we've probably truncated a snappy frame at this point
					// ok=snappy: corrupt input
					// len(dec) == 0
					//
					panic(fmt.Sprintf("could not decode snappy stream: '%s' and len dec=%d and ok=%v\n", fname, len(dec), ok))

					// get back to caller with what we've got so far
					return nEnc, nDec, nil
				}

				bnb := bytes.NewBuffer(dec)
				n, err := io.Copy(outDecodedBuf, bnb)
				if err != nil {
					panic(err)
				}
				if n != int64(len(dec)) {
					panic("could not write all bytes to outDecodedBuf")
				}
				nDec += n

				// verify the crc32 rotated checksum
				//  couldn't actually get this to match what we expected, even though it
				//  should be using the intel hardware and so be very fast.
				/*
					m32 := masked_crc32c(section)
					if m32 != crc {
						panic(fmt.Sprintf("crc32 masked failiure. expected: %v but got: %v", crc, m32))
					} else {
						fmt.Printf("\nchecksums match: %v == %v\n", crc, m32)
					}
				*/
				// move to next header
				inc := (headerSz + int(chunksz))
				chunk = chunk[inc:]
				(*encBuf).Advance(inc)
				nEnc += int64(inc)
				continue
			}
		case chunk_type == 0x01:
			{ // uncompressed data

				//n, err := w.Write(chunk[(headerSz+crc32Sz):(headerSz + int(chunksz))])
				n, err := io.Copy(outDecodedBuf, bytes.NewBuffer(chunk[(headerSz+crc32Sz):(headerSz+int(chunksz))]))
				if verbose {
					fmt.Printf("debug: n=%d  err=%v  chunksz=%d  outDecodedBuf='%v'\n", n, err, chunksz, outDecodedBuf)
				}
				if err != nil {
					panic(err)
				}
				if n != int64(chunksz-crc32Sz) {
					panic("could not write all bytes to stdout")
				}
				nDec += n

				inc := (headerSz + int(chunksz))
				chunk = chunk[inc:]
				(*encBuf).Advance(inc)
				nEnc += int64(inc)
				continue
			}
		case chunk_type == 0xfe:
			fallthrough // padding, just skip it
		case chunk_type >= 0x80 && chunk_type <= 0xfd:
			{ //  Reserved skippable chunks
				fmt.Printf("\nin reserved skippable chunks, at nEnc=%v\n", nEnc)
				inc := (headerSz + int(chunksz))
				chunk = chunk[inc:]
				nEnc += int64(inc)
				(*encBuf).Advance(inc)
				continue
			}

		default:
			panic(fmt.Sprintf("unrecognized/unsupported chunk type %s", chunk_type))
		}

	} // end for{}

	return nEnc, nDec, err
	//return int64(N), nil
}

// for whole file at once:
//
// receive on stdin (now the io.Reader, r) a stream of bytes in the snappy-streaming framed
//  format, defined here: http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
// Grab each frame, run it through the snappy decoder, and spit out
//  each frame all joined back-to-back on stdout (now on the io.Writer, w).
//
func Unsnappy(r io.Reader, w io.Writer) (err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}

	// flag for printing chunk size alignment messages
	verbose := false

	const snappyStreamHeaderSz = 10
	const headerSz = 4
	const crc32Sz = 4
	// the magic 18 bytes accounts for the snappy streaming header and the first chunks size and checksum
	// http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt

	chunk := b[:]

	// 65536 is the max size of a snappy framed chunk. See
	// http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt:91
	//buf := make([]byte, 65536)

	//	fmt.Printf("read from file, b is len:%d with value: %#v\n", len(b), b)
	//	fmt.Printf("read from file, bcut is len:%d with value: %#v\n", len(bcut), bcut)

	//fmt.Printf("raw bytes of chunksz are: %v\n", b[11:14])

	fourbytes := make([]byte, 4)
	chunkCount := 0

	for {
		if len(chunk) == 0 {
			break
		}
		chunkCount++
		fourbytes[3] = 0
		copy(fourbytes, chunk[1:4])
		chunksz := binary.LittleEndian.Uint32(fourbytes)
		chunk_type := chunk[0]

		switch true {
		case chunk_type == 0xff:
			{ // stream identifier

				streamHeader := chunk[:snappyStreamHeaderSz]
				if 0 != bytes.Compare(streamHeader, []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}) {
					panic("file had chunk starting with 0xff but then no magic snappy streaming protocol bytes, aborting.")
				} else {
					//fmt.Printf("got streaming snappy magic header just fine.\n")
				}
				chunk = chunk[snappyStreamHeaderSz:]
				continue
			}
		case chunk_type == 0x00:
			{ // compressed data
				if verbose {
					fmt.Fprintf(os.Stderr, "chunksz is %d  while  total bytes avail are: %d\n", int(chunksz), len(chunk)-4)
				}

				//crc := binary.LittleEndian.Uint32(chunk[headerSz:(headerSz + crc32Sz)])
				section := chunk[(headerSz + crc32Sz):(headerSz + chunksz)]

				dec, ok := snappy.Decode(nil, section)
				if ok != nil {
					panic("could not decode snappy stream")
				}
				//	fmt.Printf("ok, b is %#v , %#v\n", ok, dec)

				// spit out decoded text
				n, err := w.Write(dec)
				if err != nil {
					panic(err)
				}
				if n != len(dec) {
					panic("could not write all bytes to stdout")
				}

				// TODO: verify the crc32 rotated checksum?

				// move to next header
				chunk = chunk[(headerSz + int(chunksz)):]
				continue
			}
		case chunk_type == 0x01:
			{ // uncompressed data

				//crc := binary.LittleEndian.Uint32(chunk[headerSz:(headerSz + crc32Sz)])
				section := chunk[(headerSz + crc32Sz):(headerSz + chunksz)]

				n, err := w.Write(section)
				if err != nil {
					panic(err)
				}
				if n != int(chunksz-crc32Sz) {
					panic("could not write all bytes to stdout")
				}

				chunk = chunk[(headerSz + int(chunksz)):]
				continue
			}
		case chunk_type == 0xfe:
			fallthrough // padding, just skip it
		case chunk_type >= 0x80 && chunk_type <= 0xfd:
			{ //  Reserved skippable chunks
				chunk = chunk[(headerSz + int(chunksz)):]
				continue
			}

		default:
			panic(fmt.Sprintf("unrecognized/unsupported chunk type %s", chunk_type))
		}

	} // end for{}

	return nil
}

// python's implementation, for reference:

const _CHUNK_MAX = 65536
const _STREAM_TO_STREAM_BLOCK_SIZE = _CHUNK_MAX
const _STREAM_IDENTIFIER = `sNaPpY`
const _COMPRESSED_CHUNK = 0x00
const _UNCOMPRESSED_CHUNK = 0x01
const _IDENTIFIER_CHUNK = 0xff
const _RESERVED_UNSKIPPABLE0 = 0x02 // chunk ranges are [inclusive, exclusive)
const _RESERVED_UNSKIPPABLE1 = 0x80
const _RESERVED_SKIPPABLE0 = 0x80
const _RESERVED_SKIPPABLE1 = 0xff

// the minimum percent of bytes compression must save to be enabled in automatic
// mode
const _COMPRESSION_THRESHOLD = .125

var crctab *crc32.Table

func init() {
	crctab = crc32.MakeTable(crc32.Castagnoli) // this is correct table, matches the crc32c.c code used by python
}

func masked_crc32c(data []byte) uint32 {

	// see the framing format specification
	var crc uint32 = crc32.Checksum(data, crctab)
	return (((crc >> 15) | (crc << 17)) + 0xa282ead8)
}

/*
class StreamDecompressor(object):

    """This class implements the decompressor-side of the proposed Snappy
    framing format, found at

        http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
            ?spec=svn68&r=71

    This class matches a subset of the interface found for the zlib module's
    decompression objects (see zlib.decompressobj). Specifically, it currently
    implements the decompress method without the max_length option, the flush
    method without the length option, and the copy method.
    """

    __slots__ = ["_buf", "_header_found"]

    def __init__(self):
        self._buf = b""
        self._header_found = False
*/

/*
func Decompress(obuf *bytes.Buffer, data []byte, header_found bool) []byte {

	//        """Decompress 'data', returning a string containing the uncompressed
	//        data corresponding to at least part of the data in string. This data
	//        should be concatenated to the output produced by any preceding calls to
	//        the decompress() method. Some of the input data may be preserved in
	//        internal buffers for later processing.
	//        """

	fourbytes := make([]byte, 4)
	copy(obuf, data)
	uncompressed := new(bytes.Buffer)
	for {
		if len(obuf) < 4 {
			return uncompressed.Bytes()
		}

		chunk := obuf.Bytes()
		fourbytes[3] = 0
		copy(fourbytes, chunk[1:4])

		chunksz := binary.LittleEndian.Uint32(fourbytes)
		chunk_type := chunk[0]
		size = (chunk_type >> 8)
		chunk_type &= 0xff

		if !header_found {
			if chunk_type != _IDENTIFIER_CHUNK || size != len(_STREAM_IDENTIFIER) {
				panic("stream missing snappy identifier")
			}
			header_found = true
		}
		if _RESERVED_UNSKIPPABLE0 <= chunk_type && chunk_type < _RESERVED_UNSKIPPABLE1 {
			panic("stream received unskippable but unknown chunk")
		}

		if len(obuf) < 4+size {
			return uncompressed.Bytes()
		}

		chunk, obuf = obuf[4:4+size], obuf[4+size:]
		if chunk_type == _IDENTIFIER_CHUNK {
			if chunk != _STREAM_IDENTIFIER {
				panic("stream has invalid snappy identifier")
			}
			continue
		}
		if _RESERVED_SKIPPABLE0 <= chunk_type && chunk_type < _RESERVED_SKIPPABLE1 {
			continue
		}
		if chunk_type != _COMPRESSED_CHUNK && chunk_type != _UNCOMPRESSED_CHUNK {
			panic("bad chunk_type")
		}
		crc, chunk = chunk[:4], chunk[4:]
		if chunk_type == _COMPRESSED_CHUNK {
			chunk = _uncompress(chunk)

			chunk, ok := snappy.Decode(nil, chunk)
			if ok != nil {
				panic("could not snappy.Decode chunk")
			}

		}
		if masked_crc32c(chunk) != crc {
			panic("crc mismatch")
		}
		uncompressed.append(chunk)

	} // end for
}
*/
