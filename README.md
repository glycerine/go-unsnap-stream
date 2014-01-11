go-unsnap-stream
================

small golang library for decoding the snappy streaming format http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt

The crc32c checksum check on the decompression is omitted. This could be considered a speed optimization, but really it was because I couldn't get the golang crc32 library to give me back the same checksum as the python reference library.
