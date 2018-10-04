package kv.io

import java.io.{ByteArrayInputStream, InputStream}

class ByteArrayReader(bytes: Array[Byte]) extends RichInputStream {

  private val bis = new ByteArrayInputStream(bytes) {
    def position: Int = pos
  }

  def upstream: InputStream = bis

  def position: Int = bis.position
}

object ByteArrayReader {
  def apply(bytes: Array[Byte]) = new ByteArrayReader(bytes)
  def apply(bab: ByteArrayBuilder) = new ByteArrayReader(bab.toArray)
}