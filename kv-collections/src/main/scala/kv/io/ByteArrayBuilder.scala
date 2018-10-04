package kv.io

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.{Charset, StandardCharsets}
import scala.collection.mutable

class ByteArrayBuilder(initialCapacity: Int) extends RichOutputStream with mutable.Builder[Int, Array[Byte]] {

  private val bos = new ByteArrayOutputStream(initialCapacity) {
    def capacity: Int = buf.length
    def buffer: Array[Byte] = buf
  }

  def downstream: OutputStream = bos

  override def +=(b: Int): ByteArrayBuilder.this.type = { write(b); this }
  override def clear(): Unit = bos.reset()
  override def result(): Array[Byte] = bos.toByteArray

  def size: Int = bos.size
  def capacity: Int = bos.capacity

  /**
    * @return reference to the whole internal buffer (of <code>length == capacity >= size</code>)
    */
  def internalBuffer: Array[Byte] = bos.buffer

  /**
    * @return reference to the internal buffer if it is filled (i.e., <code>capacity == size</code>),
    *         or throwing an [[IllegalStateException]] if the internal buffer is not completely filled
    */
  def internalBufferAssertFull: Array[Byte] = if (capacity != size) throw new IllegalStateException("internal buffer is not completely filled") else internalBuffer

  /**
    * Resets the internal buffer to size zero, keeping capacity unchanged.
    * The builder may be used again, reusing the already allocated buffer space.
    */
  def reset(): Unit = bos.reset()

  /**
    * @return copy of the current buffer as an array
    */
  def toArray: Array[Byte] = bos.toByteArray

  /**
    * @return copy of the current buffer as a sequence
    */
  def toSeq: Seq[Byte] = toArray.toSeq

  /**
    * @return result of parsing this buffer as an UTF-8 string
    */
  override def toString: String = toString(StandardCharsets.UTF_8)

  /**
    * @return result of parsing this buffer as a string according to the given charset
    */
  def toString(charset: Charset): String = new String(internalBuffer, 0, size, charset)
}

object ByteArrayBuilder {
  val DefaultInitialCapacity = 32
  def apply(initialCapacity: Int = DefaultInitialCapacity): ByteArrayBuilder = new ByteArrayBuilder(initialCapacity)
  def create[T](obj: T*)(implicit binarizer: Binarizer[T]): ByteArrayBuilder = {
    obj.foldLeft(ByteArrayBuilder()) { case (bab, o) => bab.append(o) }
  }
}