# OS dependent file operation
fs = require 'fs'

class YadbFS
  @SIGNATURE_SIZE: 1

  constructor: (path) ->
    path = '../' + path unless fs.existsSync(path)  # FIXME dirty hack
    @handle = fs.openSync(path , 'r') 
    @stat = fs.fstatSync @handle
    @size = @stat.size

  readSignature: (pos) ->
    buf = new Buffer YadbFS.SIGNATURE_SIZE
    fs.readSync(@handle, buf, 0, YadbFS.SIGNATURE_SIZE, pos)
    buf.toString('utf8', 0, YadbFS.SIGNATURE_SIZE)

  readString: (pos, blocksize, encoding) ->
    buf = new Buffer blocksize
    fs.readSync(@handle, buf, 0, blocksize, pos)
    buf.toString(encoding ? 'utf8')

  readStringArray: (pos, blocksize, encoding) ->
    return [] if blocksize == 0
    buf = new Buffer blocksize
    fs.readSync(@handle, buf, 0, blocksize, pos)
    buf.toString(encoding ? 'utf8').split('\0')

  readUInt32: (pos) ->
    buf = new Buffer 4
    fs.readSync(@handle, buf, 0, 4, pos)
    buf.readUInt32BE 0

  readInt32: (pos) ->
    buf = new Buffer 4
    fs.readSync(@handle, buf, 0, 4, pos)
    buf.readInt32BE 0

  readUInt8: (pos) ->
    buf = new Buffer 1
    fs.readSync(@handle, buf, 0, 1, pos)
    buf.readUInt8 0

  readBuf: (pos, blocksize) ->
    buf = new Buffer blocksize
    fs.readSync(@handle, buf, 0, blocksize, pos)
    buf

  readPackedIntBuf: (pos, blocksize, count, reset) ->
    buf = @readBuf(pos, blocksize)
    @_unpackInt(buf, count, reset)

  readFixedArray: (pos, count, unitsize) ->
    bufsize = unitsize * count
    throw "array size exceeds file size" if bufsize > @size && @size
    items = new Buffer bufsize
    switch unitsize
      when 1 then func = items.readUInt8
      when 2 then func = items.readUInt16BE
      when 4 then func = items.readUInt32BE
      else throw "unsupported integer size"
    fs.readSync(@handle, items, 0, bufsize, pos)
    func.call(items, i * unitsize) for i in [0..items.length / unitsize] by 1

  free: -> fs.closeSync @handle

  _unpackInt: (ar, count, reset) ->
    count or= ar.length
    r = []; i = v = 0

    loop
      shift = 0
      loop
        v += (ar[i] & 0x7F) << shift
        shift += 7
        break unless ar[++i] & 0x80

      r.push v
      v = 0 if reset
      count--
      break unless i < ar.length && count

    { data: r, adv: i }

  # compatibility with old code
  readUI8: @::readUInt8
  readUI32: @::readUInt32
  readI32: @::readInt32 
  readBuf_packedint: @::readPackedIntBuf
  signature_size: @SIGNATURE_SIZE


module.exports = YadbFS
