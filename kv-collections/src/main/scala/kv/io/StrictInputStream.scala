package kv.io

import java.io.{EOFException, InputStream}

/**
  * In contrast to a standard InputStream, this stream guarantees the following two assertions, which allow to
  * significantly simplify downstream logic:
  * <ol>
  *   <li><code>read()</code> returns a successfully read unsigned byte (0..255) or throws an EOFException (rather than returning -1)</li>
  *   <li><code>read(array: Array[Byte])</code> and <code>read(array: Array[Byte], off: Int, len: Int)</code> return a
  *       completely filled array or fail with an EOFException (rather than returning a partially filled array)</li>
  * <ol>
  */
trait StrictInputStream extends InputStream {

  def upstream: InputStream

  def read(): Int = {
    val ubyte = upstream.read()
    if (ubyte < 0)
      throw new EOFException()
    else
      ubyte
  }

  override def read(array: Array[Byte]): Int = read(array, 0, array.length)

  override def read(array: Array[Byte], off: Int, len: Int): Int = {
    // see java.io.DataInputStream.readFully
    var n = 0
    while (n < len) {
      val count = upstream.read(array, off + n, len - n)
      if (count < 0)
        throw new EOFException()
      n += count
    }
    n
  }

  def read(n: Int): Array[Byte] = {val array = Array.ofDim[Byte](n); read(array); array}

  override def available(): Int = upstream.available()
  override def close(): Unit = upstream.close()
  override def mark(readlimit: Int): Unit = upstream.mark(readlimit)
  override def markSupported(): Boolean = upstream.markSupported()
}

object StrictInputStream {
  def apply(stream: InputStream): StrictInputStream = new StrictInputStream { val upstream: InputStream = stream }
}