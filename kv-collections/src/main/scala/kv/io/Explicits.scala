package kv.io

import java.nio.charset.{Charset, StandardCharsets}
import java.time.Instant

import scala.reflect.ClassTag

import scala.collection.mutable

object Explicits {

  /*
   * Binarizers for basic data types
   */

  val boolean = new StreamBinarizer[Boolean] {
    override def write(out: RichOutputStream, b: Boolean): Unit = out.write(if (b) 1 else 0)
    override def read(in: RichInputStream): Boolean = in.read() != 0
  }

  val byte = new StreamBinarizer[Byte] {
    override def write(out: RichOutputStream, b: Byte): Unit = out.write(b)
    override def read(in: RichInputStream): Byte = in.read().toByte
  }

  val short = new StreamBinarizer[Short] {
    override def write(out: RichOutputStream, s: Short): Unit = { out.write(s >>> 8); out.write(s) }
    override def read(in: RichInputStream): Short = { (in.read() << 8 | in.read()).toShort }
  }

  val char = new StreamBinarizer[Char] {
    override def write(out: RichOutputStream, c: Char): Unit = { out.write(c >>> 8); out.write(c.toInt) }
    override def read(in: RichInputStream): Char = { (in.read() << 8 | in.read()).toChar }
  }

  val int = new StreamBinarizer[Int] {
    override def write(out: RichOutputStream, i: Int): Unit = { out.write(i >>> 24); out.write(i >>> 16); out.write(i >>> 8); out.write(i) }
    override def read(in: RichInputStream): Int = { (in.read() << 24) | (in.read() << 16) | (in.read() << 8) | in.read() }
  }

  val intSortable = new StreamBinarizer[Int] {
    override def write(out: RichOutputStream, i: Int): Unit = { int.write(out, i - 0x80000000) }
    override def read(in: RichInputStream): Int = { int.read(in) + 0x80000000 }
  }

  val long = new StreamBinarizer[Long] {
    def write(out: RichOutputStream, l: Long): Unit = {
      out.write((l >>> 56).toByte)
      out.write((l >>> 48).toByte)
      out.write((l >>> 40).toByte)
      out.write((l >>> 32).toByte)
      out.write((l >>> 24).toByte)
      out.write((l >>> 16).toByte)
      out.write((l >>>  8).toByte)
      out.write(l.toByte)
    }
    def read(in: RichInputStream): Long = {
      (in.read().toLong << 56) |
        (in.read().toLong << 48) |
        (in.read().toLong << 40) |
        (in.read().toLong << 32) |
        (in.read().toLong << 24) |
        (in.read() << 16) |
        (in.read() << 8) |
        in.read()
    }
  }

  val longSortable = new StreamBinarizer[Long] {
    override def write(out: RichOutputStream, l: Long): Unit = { long.write(out, l - 0x8000000000000000L) }
    override def read(in: RichInputStream): Long = { long.read(in) + 0x8000000000000000L }
  }

  val varlen = new StreamBinarizer[Long] {
    def write(out: RichOutputStream, l: Long): Unit = VarlenZigzagCoding.writeVarlenZizag(l, out)
    def read(in: RichInputStream): Long = VarlenZigzagCoding.readVarlenZigzag(in)
  }

  val varlenNN = new StreamBinarizer[Long] {
    def write(out: RichOutputStream, l: Long): Unit = VarlenZigzagCoding.writeVarlen(l, out)
    def read(in: RichInputStream): Long = VarlenZigzagCoding.readVarlen(in)
  }

  val varlenInt = new StreamBinarizer[Int] {
    def write(out: RichOutputStream, i: Int): Unit = VarlenZigzagCoding.writeVarlenZizag(i, out)
    def read(in: RichInputStream): Int = {
      val l = VarlenZigzagCoding.readVarlenZigzag(in)
      if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE)
        throw new IllegalArgumentException(s"read variable-length-encoded $l is outside Int range")
      else
        l.toInt
    }
  }

  val varlenIntNN = new StreamBinarizer[Int] {
    def write(out: RichOutputStream, i: Int): Unit = VarlenZigzagCoding.writeVarlen(i, out)
    def read(in: RichInputStream): Int = {
      val l = VarlenZigzagCoding.readVarlen(in)
      if (l > Integer.MAX_VALUE) // cannot be negative
        throw new IllegalArgumentException(s"read variable-length-encoded $l is outside Int range")
      else
        l.toInt
    }
  }

  val float = new StreamBinarizer[Float] {
    def write(out: RichOutputStream, f: Float): Unit = int.write(out, java.lang.Float.floatToRawIntBits(f))
    def read(in: RichInputStream): Float = java.lang.Float.intBitsToFloat(int.read(in))
  }

  val double = new StreamBinarizer[Double] {
    def write(out: RichOutputStream, d: Double): Unit = long.write(out, java.lang.Double.doubleToRawLongBits(d))
    def read(in: RichInputStream): Double = java.lang.Double.longBitsToDouble(long.read(in))
  }

  val DefaultCharset = StandardCharsets.UTF_8
  val DefaultTermByte = 0

  def string(charset: Charset = DefaultCharset) = new StreamBinarizer[String] {
    def write(out: RichOutputStream, s: String): Unit = {
      val bytes = s.getBytes(charset)
      varlenIntNN.write(out, bytes.length)
      out.write(bytes)
    }
    def read(in: RichInputStream): String = new String(in.read(varlenIntNN.read(in)), charset)
  }

  def stringTerm(termByte: Int = DefaultTermByte, charset: Charset = DefaultCharset) = new StreamBinarizer[String] {
    def write(out: RichOutputStream, s: String): Unit = {
      require(termByte >= 0 && termByte <= 127, s"terminating byte must stay in range [0,127], but is $termByte")
      val bytes = s.getBytes(charset) // bytes does not contain termByte iff termByte is not contained in s
      require(!bytes.contains(termByte.toByte), s"terminating byte ($termByte) must not be contained in the string, but is: $s")
      out.write(bytes)
      out.write(termByte)
    }
    def read(in: RichInputStream): String = {
      require(termByte >= 0 && termByte <= 127, s"terminating byte must stay in range [0,127], but is $termByte")
      val bab = ByteArrayBuilder()
      var resume = true
      while (resume) {
        val ubyte = in.read()
        if (ubyte != termByte)
          bab += ubyte
        else
          resume = false
      }
      new String(bab.result, StandardCharsets.UTF_8)
    }
  }

  val utf8 = string(DefaultCharset)
  val utf8Term = stringTerm(DefaultTermByte, DefaultCharset)

  val instant = new StreamBinarizer[Instant] {
    override def write(out: RichOutputStream, time: Instant): Unit = long.write(out, time.toEpochMilli)
    override def read(in: RichInputStream): Instant = Instant.ofEpochMilli(long.read(in))
  }

  val instantSortable = new StreamBinarizer[Instant] {
    override def write(out: RichOutputStream, time: Instant): Unit = longSortable.write(out, time.toEpochMilli)
    override def read(in: RichInputStream): Instant = Instant.ofEpochMilli(longSortable.read(in))
  }

  /*
   * Binarizers for containers and collections
   */

  def option[T](implicit binarizer: Binarizer[T]) = new StreamBinarizer[Option[T]] {
    override def write(out: RichOutputStream, opt: Option[T]): Unit = {
      opt match {
        case Some(value) => boolean.write(out, true); binarizer.write(out, value)
        case None => boolean.write(out, false)
      }
    }
    override def read(in: RichInputStream): Option[T] = {
      if (boolean.read(in)) {
        Some(binarizer.read(in))
      } else {
        None
      }
    }
  }

  def some[T](implicit binarizer: Binarizer[T]) = new StreamBinarizer[Some[T]] {
    override def write(out: RichOutputStream, some: Some[T]): Unit = {
      boolean.write(out, true)
      binarizer.write(out, some.get)
    }
    override def read(in: RichInputStream): Some[T] = {
      if (boolean.read(in)) {
        Some(binarizer.read(in))
      } else {
        throw new IllegalArgumentException("cannot read Some (but None) from input stream")
      }
    }
  }

  val byteArray = new StreamBinarizer[Array[Byte]] {
    override def write(out: RichOutputStream, array: Array[Byte]): Unit = {
      varlenIntNN.write(out, array.length)
      out.write(array)
    }
    override def read(in: RichInputStream): Array[Byte] = in.read(varlenIntNN.read(in))
  }

  def array[T:ClassTag](implicit binarizer: Binarizer[T]) = new StreamBinarizer[Array[T]] {
    override def write(out: RichOutputStream, array: Array[T]): Unit = {
      varlenIntNN.write(out, array.length)
      array.foreach(e => binarizer.write(out, e))
    }
    override def read(in: RichInputStream): Array[T] = {
      (1 to varlenIntNN.read(in)).map(_ => binarizer.read(in)).toArray
    }
  }

  def seq[T](implicit binarizer: Binarizer[T]) = new StreamBinarizer[Seq[T]] {
    override def write(out: RichOutputStream, seq: Seq[T]): Unit = {
      varlenIntNN.write(out, seq.size)
      seq.foreach(e => binarizer.write(out, e))
    }
    override def read(in: RichInputStream): Seq[T] = {
      (1 to varlenIntNN.read(in)).map(_ => binarizer.read(in))
    }
  }

  def set[T](implicit binarizer: Binarizer[T]) = new StreamBinarizer[Set[T]] {
    override def write(out: RichOutputStream, set: Set[T]): Unit = {
      varlenIntNN.write(out, set.size)
      set.foreach(e => binarizer.write(out, e))
    }
    override def read(in: RichInputStream): Set[T] = {
      (1 to varlenIntNN.read(in)).map(_ => binarizer.read(in)).toSet
    }
  }

  def mutableSet[T](implicit binarizer: Binarizer[T]) = new StreamBinarizer[mutable.Set[T]] {
    override def write(out: RichOutputStream, set: mutable.Set[T]): Unit = {
      varlenIntNN.write(out, set.size)
      set.foreach(e => binarizer.write(out, e))
    }
    override def read(in: RichInputStream): mutable.Set[T] = {
      mutable.Set((1 to varlenIntNN.read(in)).map(_ => binarizer.read(in)):_*)
    }
  }

  def map[K,V](implicit keyBinarizer: Binarizer[K], valueBinarizer: Binarizer[V]) = new StreamBinarizer[Map[K,V]] {
    override def write(out: RichOutputStream, map: Map[K,V]): Unit = {
      varlenIntNN.write(out, map.size)
      map.foreach { case (key, value) => keyBinarizer.write(out, key); valueBinarizer.write(out, value) }
    }
    override def read(in: RichInputStream): Map[K,V] = {
      (1 to varlenIntNN.read(in)).map(_ => (keyBinarizer.read(in), valueBinarizer.read(in))).toMap
    }
  }

  def mutableMap[K,V](implicit keyBinarizer: Binarizer[K], valueBinarizer: Binarizer[V]) = new StreamBinarizer[mutable.Map[K,V]] {
    override def write(out: RichOutputStream, map: mutable.Map[K,V]): Unit = {
      varlenIntNN.write(out, map.size)
      map.foreach { case (key, value) => keyBinarizer.write(out, key); valueBinarizer.write(out, value) }
    }
    override def read(in: RichInputStream): mutable.Map[K,V] = {
      mutable.Map((1 to varlenIntNN.read(in)).map(_ => (keyBinarizer.read(in), valueBinarizer.read(in))):_*)
    }
  }

  /*
   * Tuples
   */

  def tuple2[A,B](implicit a: Binarizer[A], b: Binarizer[B]) = new StreamBinarizer[(A,B)] {
    override def write(out: RichOutputStream, t: (A,B)): Unit = {
      a.write(out, t._1); b.write(out, t._2)
    }
    override def read(in: RichInputStream): (A,B) = (
      a.read(in), b.read(in)
    )
  }

  def tuple3[A,B,C](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C]) = new StreamBinarizer[(A,B,C)] {
    override def write(out: RichOutputStream, t: (A,B,C)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3)
    }
    override def read(in: RichInputStream): (A,B,C) = (
      a.read(in), b.read(in), c.read(in)
    )
  }

  def tuple4[A,B,C,D](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D]) = new StreamBinarizer[(A,B,C,D)] {
    override def write(out: RichOutputStream, t: (A,B,C,D)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
    }
    override def read(in: RichInputStream): (A,B,C,D) = (
      a.read(in), b.read(in), c.read(in), d.read(in)
    )
  }

  def tuple5[A,B,C,D,E](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                        e: Binarizer[E]) = new StreamBinarizer[(A,B,C,D,E)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5)
    }
    override def read(in: RichInputStream): (A,B,C,D,E) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in)
    )
  }

  def tuple6[A,B,C,D,E,F](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                          e: Binarizer[E], f: Binarizer[F]) = new StreamBinarizer[(A,B,C,D,E,F)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in)
    )
  }

  def tuple7[A,B,C,D,E,F,G](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                            e: Binarizer[E], f: Binarizer[F], g: Binarizer[G]) = new StreamBinarizer[(A,B,C,D,E,F,G)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F,G)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6); g.write(out, t._7)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F,G) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in), g.read(in)
    )
  }

  def tuple8[A,B,C,D,E,F,G,H](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                              e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H]) = new StreamBinarizer[(A,B,C,D,E,F,G,H)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F,G,H)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6); g.write(out, t._7); h.write(out, t._8)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F,G,H) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in), g.read(in), h.read(in)
    )
  }

  def tuple9[A,B,C,D,E,F,G,H,I](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                i: Binarizer[I]) = new StreamBinarizer[(A,B,C,D,E,F,G,H,I)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F,G,H,I)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6); g.write(out, t._7); h.write(out, t._8)
      i.write(out, t._9)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F,G,H,I) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in), g.read(in), h.read(in),
      i.read(in)
    )
  }

  def tuple10[A,B,C,D,E,F,G,H,I,J](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                   e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                   i: Binarizer[I], j: Binarizer[J]) = new StreamBinarizer[(A,B,C,D,E,F,G,H,I,J)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F,G,H,I,J)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6); g.write(out, t._7); h.write(out, t._8)
      i.write(out, t._9); j.write(out, t._10)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F,G,H,I,J) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in), g.read(in), h.read(in),
      i.read(in), j.read(in)
    )
  }

  def tuple11[A,B,C,D,E,F,G,H,I,J,K](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                     e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                     i: Binarizer[I], j: Binarizer[J], k: Binarizer[K]) = new StreamBinarizer[(A,B,C,D,E,F,G,H,I,J,K)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F,G,H,I,J,K)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6); g.write(out, t._7); h.write(out, t._8)
      i.write(out, t._9); j.write(out, t._10); k.write(out, t._11)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F,G,H,I,J,K) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in), g.read(in), h.read(in),
      i.read(in), j.read(in), k.read(in)
    )
  }

  def tuple12[A,B,C,D,E,F,G,H,I,J,K,L](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                      e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                      i: Binarizer[I], j: Binarizer[J], k: Binarizer[K], l: Binarizer[L]) = new StreamBinarizer[(A,B,C,D,E,F,G,H,I,J,K,L)] {
    override def write(out: RichOutputStream, t: (A,B,C,D,E,F,G,H,I,J,K,L)): Unit = {
      a.write(out, t._1); b.write(out, t._2); c.write(out, t._3); d.write(out, t._4)
      e.write(out, t._5); f.write(out, t._6); g.write(out, t._7); h.write(out, t._8)
      i.write(out, t._9); j.write(out, t._10); k.write(out, t._11); l.write(out, t._12)
    }
    override def read(in: RichInputStream): (A,B,C,D,E,F,G,H,I,J,K,L) = (
      a.read(in), b.read(in), c.read(in), d.read(in),
      e.read(in), f.read(in), g.read(in), h.read(in),
      i.read(in), j.read(in), k.read(in), l.read(in)
    )
  }
}
