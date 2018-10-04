package kv.io

import java.io.OutputStream
import java.util

/**
  * This object provides functionality to represent a long number using variable length encoding (for non-negative longs),
  * plus optionally zig zag encoding (for positive or negative longs).
  * These encodings allow to represent the original 8 bytes by 1-10 bytes, where the encoded length depends on the
  * absolute value of the number. For further explanations see for example:
  *
  * <ul>
  *   <li>https://github.com/apache/avro/blob/482adcfa1857033f2a76b07b429a266b0231f433/lang/c%2B%2B/impl/Zigzag.cc</li>
  *   <li>https://techoverflow.net/blog/2013/01/25/efficiently-encoding-variable-length-integers-in-cc/</li>
  * </ul>
  */

object VarlenZigzagCoding {

  /**
    * @param l long value to encode (consider using [[toVarlen()]] for non-negative numbers)
    * @return the varlen-zigzag encoding of <code>l</code>, consisting of 1 to 10 bytes.
    */
  def toVarlenZizag(l: Long): Array[Byte] = ulong2varlen(long2zigzag(l))

  /**
    * @param bytes the byte array from which to extract a varlen-zigzag encoded number
    * @return the encoded long value
    */
  def readVarlenZigzag(bytes: Array[Byte]): Long = zigzag2long(varlen2ulong(bytes))

  /**
    * @param l non-negative long value to encode (use [[toVarlenZizag()]] for negative numbers at the price of slightly more space consumption)
    * @return the varlen encoding of <code>l</code>, consisting of 1 to 10 bytes.
    * @throws IllegalArgumentException if the input is negative
    */
  def toVarlen(l: Long): Array[Byte] = {
    require(l >= 0, s"for varlen encoding, the number must not be negative, but is $l. Use varlen-zizag encoding instead.")
    ulong2varlen(l)
  }

  /**
    * @param bytes the byte array from which to extract a varlen encoded number
    * @return the encoded long value
    * @throws IllegalArgumentException if the decoded number is negative (only non-negative number can be varlen encoded)
    */
  def readVarlen(bytes: Array[Byte]): Long = {
    val l = varlen2ulong(bytes)
    require(l >= 0, s"decoded value is negative ($l), although expected to be non-negative. Probably was encoded by varlen-zigzag?")
    l
  }

  def writeVarlen(l: Long, out: OutputStream): Unit = {
    require(l >= 0, s"for varlen encoding, the number must not be negative, but is $l. Use varlen-zizag encoding instead.")
    ulong2varlen(l, Some(out))
  }

  def readVarlen(in: StrictInputStream): Long = {
    val l = varlen2ulong(in)
    require(l >= 0, s"decoded value is negative ($l), although expected to be non-negative. Probably was encoded by varlen-zigzag?")
    l
  }

  def writeVarlenZizag(v: Long, out: OutputStream): Array[Byte] = ulong2varlen(long2zigzag(v), Some(out))

  def readVarlenZigzag(in: StrictInputStream): Long = zigzag2long(varlen2ulong(in))

  /*
   * PRIVATE HELPERS
   */

  private def long2zigzag(v: Long): Long = (v << 1) ^ (v >> 63)

  private def zigzag2long(v: Long): Long = (v >>> 1) ^ -(v & 1)

  private def ulong2varlen(ul: Long, out: Option[OutputStream] = None): Array[Byte] = {
    var value = ul
    val bytes = Array.ofDim[Byte](10)
    bytes(0) = value.toByte
    var size = 1
    value >>>= 7
    while (value != 0) {
      bytes(size-1) = (bytes(size-1) | 0x80).toByte
      bytes(size) = value.toByte
      size += 1
      value >>>= 7
    }

    if (out.isDefined) {
      out.get.write(bytes, 0, size)
      null
    } else {
      util.Arrays.copyOf(bytes, size)
    }
  }

  private def varlen2ulong(bytes: Array[Byte]): Long = {
    var ul = 0L
    for (i <- bytes.indices) {
      ul |= (bytes(i) & 0x7F.toByte).toLong << (7*i)
      if ((bytes(i) & 0x80.toByte) == 0)
        return ul
    }
    throw new IllegalStateException("reached 'unreachable' state")
  }

  private def varlen2ulong(reader: StrictInputStream): Long = {
    var ul = 0L
    for (i <- 0 until 10) {
      val b = reader.read()
      ul |= (b & 0x7F.toByte).toLong << (7*i)
      if ((b & 0x80.toByte) == 0)
        return ul
    }
    throw new IllegalStateException("reached 'unreachable' state")
  }
}
