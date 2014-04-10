go-unsnap-stream
================

This is a small golang library for decoding and encoding the snappy *streaming* format, specified here: http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt

Note that the *streaming format* for snappy is different from snappy itself. Think of it as a train of boxcars: the streaming format breaks your data in chunks, applies snappy to each chunk alone, then puts a thin wrapper around the chunk, and sends it along in turn. You can begin decoding before receiving everything. And memory requirements for decoding are sane.

Strangely, though the streaming format was first proposed in Go[1][2], it was never upated, and I could not locate any other library for go that would handle the streaming/framed snappy format. Hence this implementation of the spec. There is a command line tool[3] that has a C implementation, but this is the only Go implementation that I am aware of. The reference for the framing/streaming spec seems to be the python implementation[4].

For binary compatibility with the python implementation, we call the C-snappy compressor/decompressor code directly; using github.com/dgryski/go-csnappy. The native Go implementation produces slightly different binary compression, making test-diffs harder, and some have complained about it being slower than the C.

[1] https://groups.google.com/forum/#!msg/snappy-compression/qvLNe2cSH9s/R19oBC-p7g4J

[2] https://codereview.appspot.com/5167058

[3] https://github.com/kubo/snzip

[4] https://pypi.python.org/pypi/python-snappy