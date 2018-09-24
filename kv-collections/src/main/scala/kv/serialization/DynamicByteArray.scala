package kv.serialization

import java.io._
import java.nio.charset.{Charset, StandardCharsets}

/**
  * A re-implementation of [[ByteArrayOutputStream]] with unsynchronized writes (more efficient) plus additional access methods.
  */

class DynamicByteArray(initialCapacity: Int) extends OutputStream {
  require(initialCapacity >= 0, "initial capacity must not be negative, but is " + initialCapacity)

  private[this] var buf = Array.ofDim[Byte](initialCapacity)
  private[this] var count: Int = 0

  private def ensureCapacity(minCapacity: Int): Unit = {
    if (minCapacity > buf.length) {
      val oldCapacity = buf.length
      val newCapacity = minCapacity max (oldCapacity << 1)
      buf = java.util.Arrays.copyOf(buf, newCapacity)
    }
  }

  def write(b: Int): Unit = {
    ensureCapacity(count + 1)
    buf(count) = b.toByte
    count += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0))
      throw new IndexOutOfBoundsException

    ensureCapacity(count + len)
    System.arraycopy(b, off, buf, count, len)
    count += len
  }

  def writeTo(out: OutputStream): Unit = out.write(buf, 0, count)

  /**
    * @return reference to the internal buffer (of <code>length >= size</code>)
    */
  def getBuffer: Array[Byte] = buf

  /**
    * @return reference to the internal buffer, or throwing an [[IllegalStateException]] if the buffer is not completely filled
    */
  def getBufferAssertFull: Array[Byte] = if (count != buf.length) throw new IllegalStateException("buffer is not completely filled") else buf

  /**
    * Clears this buffer (and resets its capacity if <code>resetToInitialCapacity</code> is explicitly set to <code>true</code>).
    * @param resetToInitialCapacity whether to create a new buffer of initial size (<code>true</code>) or to re-use the current buffer (<code>false</code>, default) with its current capacity
    */
  def reset(resetToInitialCapacity: Boolean = false): Unit = {
    count = 0
    if (resetToInitialCapacity && buf.length > initialCapacity)
      buf = Array.ofDim[Byte](initialCapacity)
  }

  def size: Int = count

  def capacity: Int = buf.length

  def toSeq(returnBufferIfFull: Boolean = false): Seq[Byte] = toArray(returnBufferIfFull).toSeq

  def toArray(returnBufferIfFull: Boolean = false): Array[Byte] = if (returnBufferIfFull && count == buf.length) buf else java.util.Arrays.copyOf(buf, count)

  def toString(charset: Charset = StandardCharsets.UTF_8): String = new String(buf, 0, count, charset)
}

object DynamicByteArray {
  val DefaultInitialCapacity = 32
  def apply(initialSize: Int = DefaultInitialCapacity): DynamicByteArray = new DynamicByteArray(initialSize)
}