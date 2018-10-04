package kv.io

import java.net.URL
import java.time.Instant

import org.scalatest.FunSuite

import scala.collection.mutable

class RichInputOutputStreamTest extends FunSuite {

  private val booleanValues = Seq[Boolean](false,true)
  private val byteValues = Seq[Byte](0, 1, -1, 100, -100, Byte.MinValue, Byte.MaxValue)
  private val shortValues = Seq[Short](0, 1, -1, 100, -100, 10000, -10000, Short.MinValue, Short.MaxValue)
  private val charValues = Seq[Char](0.toChar, ' ', 'x', '\u1234', Char.MinValue, Char.MaxValue)
  private val intValues = Seq[Int](0, 1, -1, 1000, -1000, 100000, -100000, 1000000000, -1000000000, Int.MinValue, Int.MaxValue)
  private val longValues = Seq[Long](0, 1, -1, 1000, -1000, 1000000000L, -1000000000L, 1000000000000000000L, -1000000000000000000L, Long.MinValue, Long.MaxValue)
  private val floatValues = Seq[Float](0f, 1f, -1f, 1000f, -1000f, 1.2345E10f, -1.2345E10f, Float.PositiveInfinity, Float.MaxValue, Float.MinPositiveValue, -Float.MinPositiveValue, Float.MinValue, Float.NegativeInfinity)
  private val doubleValues = Seq[Double](0, 1, -1, 1000, -1000, 1.2345E10, -1.2345E10, Double.PositiveInfinity, Double.MaxValue, Double.MinPositiveValue, -Double.MinPositiveValue, Double.MinValue, Double.NegativeInfinity)
  private val timeValues: Seq[Instant] = (longValues ++ Seq(Instant.now().toEpochMilli)).map(Instant.ofEpochMilli)
  private val stringValues = Seq[String]("", "A", "ABC", "äöü!\"§$%&/()=?\u1234", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"*100)

  def toOptionSeq[T](seq: Seq[T]): Seq[Option[T]] = Seq(None) ++ seq.map(x => Some(x))

  test("basic serialization & deserialization") {

    import Implicits.default._
    import Explicits._

    for (b <- booleanValues)
      assert(b == ByteArrayReader(ByteArrayBuilder(1).append(b).internalBufferAssertFull).get[Boolean]())

    for (b <- toOptionSeq(booleanValues))
      assert(b == ByteArrayReader(ByteArrayBuilder.create(b)).get[Option[Boolean]]())

    for (b <- byteValues)
      assert(b == ByteArrayReader(ByteArrayBuilder(1).append(b).internalBufferAssertFull).get[Byte]())

    for (b <- toOptionSeq(byteValues))
      assert(b == ByteArrayReader(ByteArrayBuilder.create(b)).get[Option[Byte]]())

    assert(byteValues.toArray sameElements ByteArrayReader(ByteArrayBuilder.create(byteValues.toArray)).get[Array[Byte]]())
    assert(byteValues.toArray sameElements ByteArrayReader(ByteArrayBuilder.create(Option(byteValues.toArray))).get[Option[Array[Byte]]]().get)

    for (s <- shortValues)
      assert(s == ByteArrayReader(ByteArrayBuilder(2).append(s).internalBufferAssertFull).get[Short]())

    for (s <- toOptionSeq(shortValues))
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)).get[Option[Short]]())

    for (c <- charValues)
      assert(c == ByteArrayReader(ByteArrayBuilder(2).append(c).internalBufferAssertFull).get[Char]())

    for (c <- toOptionSeq(charValues))
      assert(c == ByteArrayReader(ByteArrayBuilder.create(c)).get[Option[Char]]())

    for (i <- intValues) {
      assert(i == ByteArrayReader(ByteArrayBuilder(4).append(i).internalBufferAssertFull).get[Int]())
      assert(i == ByteArrayReader(ByteArrayBuilder(4).append(i)(int).internalBufferAssertFull).get[Int]()(int))
      assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(intSortable)).get[Int]()(intSortable))
      assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(varlenInt)).get[Int]()(varlenInt))

      if (i >= 0) {
        assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(varlenIntNN)).get[Int]()(varlenIntNN))
      }
    }

    for (i <- toOptionSeq(intValues)) {
      assert(i == ByteArrayReader(ByteArrayBuilder.create(i)).get[Option[Int]]())
      assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(option[Int](int))).get[Option[Int]]()(option[Int](int)))
      assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(option[Int](intSortable))).get[Option[Int]]()(option[Int](intSortable)))
      assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(option[Int](varlenInt))).get[Option[Int]]()(option[Int](varlenInt)))

      if (i.isEmpty || i.get >= 0) {
        assert(i == ByteArrayReader(ByteArrayBuilder.create(i)(option[Int](varlenIntNN))).get[Option[Int]]()(option[Int](varlenIntNN)))
      }
    }

    for (l <- longValues) {
      assert(l == ByteArrayReader(ByteArrayBuilder(8).append(l).internalBufferAssertFull).get[Long]())
      assert(l == ByteArrayReader(ByteArrayBuilder(4).append(l)(long).internalBufferAssertFull).get[Long]()(long))
      assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(longSortable)).get[Long]()(longSortable))
      assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(varlen)).get[Long]()(varlen))

      if (l >= 0) {
        assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(varlenNN)).get[Long]()(varlenNN))
      }
    }

    for (l <- toOptionSeq(longValues)) {
      assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(option[Long](long))).get[Option[Long]]()(option[Long](long)))
      assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(option[Long](longSortable))).get[Option[Long]]()(option[Long](longSortable)))
      assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(option[Long](varlen))).get[Option[Long]]()(option[Long](varlen)))

      if (l.isEmpty || l.get >= 0) {
        assert(l == ByteArrayReader(ByteArrayBuilder.create(l)(option[Long](varlenNN))).get[Option[Long]]()(option[Long](varlenNN)))
      }
    }

    for (f <- floatValues) {
      assert(f == ByteArrayReader(ByteArrayBuilder(4).append(f).internalBufferAssertFull).get[Float]())
    }

    assert(ByteArrayReader(ByteArrayBuilder(4).append(Float.NaN).internalBufferAssertFull).get[Float]().toDouble.isNaN)

    for (f <- toOptionSeq(floatValues)) {
      assert(f == ByteArrayReader(ByteArrayBuilder.create(f)).get[Option[Float]]())
    }

    assert(ByteArrayReader(ByteArrayBuilder.create(Some(Float.NaN))).get[Option[Float]]().get.toDouble.isNaN)

    for (d <- doubleValues) {
      assert(d == ByteArrayReader(ByteArrayBuilder(8).append(d).internalBufferAssertFull).get[Double]())
    }

    assert(ByteArrayReader(ByteArrayBuilder(8).append(Double.NaN).internalBufferAssertFull).get[Double]().isNaN)

    for (d <- toOptionSeq(doubleValues)) {
      assert(d == ByteArrayReader(ByteArrayBuilder.create(d)).get[Option[Double]]())
    }

    assert(ByteArrayReader(ByteArrayBuilder.create(Some(Double.NaN))).get[Option[Double]]().get.isNaN)

    for (s <- stringValues) {
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)).get[String]())
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)(utf8)).get[String]()(utf8))
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)(utf8Term)).get[String]()(utf8Term))
    }

    for (s <- toOptionSeq(stringValues)) {
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)).get[Option[String]]())
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)(option[String](utf8))).get[Option[String]]()(option[String](utf8)))
      assert(s == ByteArrayReader(ByteArrayBuilder.create(s)(option[String](utf8Term))).get[Option[String]]()(option[String](utf8Term)))
    }

    for (time <- timeValues) {
      assert(time == ByteArrayReader(ByteArrayBuilder(8).append(time).internalBufferAssertFull).get[Instant]())
      assert(time == ByteArrayReader(ByteArrayBuilder(8).append(time)(instant).internalBufferAssertFull).get[Instant]()(instant))
      assert(time == ByteArrayReader(ByteArrayBuilder(8).append(time)(instantSortable).internalBufferAssertFull).get[Instant]()(instantSortable))
    }

    for (time <- toOptionSeq(timeValues)) {
      assert(time == ByteArrayReader(ByteArrayBuilder.create(time)).get[Option[Instant]]())
      assert(time == ByteArrayReader(ByteArrayBuilder.create(time)(option[Instant](instant))).get[Option[Instant]]()(option[Instant](instant)))
    }
  }

  test("implicit collection serialization & deserialization") {
    import Implicits.default._

    {
      val array = Array(1, 2, 3).map(_.toByte)
      assert(array sameElements ByteArrayReader(ByteArrayBuilder.create(array)).get[Array[Byte]])
    }

    {
      val array = Array("a", "b", "c")
      assert(array sameElements ByteArrayReader(ByteArrayBuilder.create(array)).get[Array[String]])
    }

    {
      val seq = Seq("a", "b", "c")
      assert(seq == ByteArrayReader(ByteArrayBuilder.create(seq)).get[Seq[String]])
    }

    {
      val set = Set("a", "b", "c")
      assert(set == ByteArrayReader(ByteArrayBuilder.create(set)).get[Set[String]])
    }

    {
      val set = mutable.Set("a", "b", "c")
      assert(set == ByteArrayReader(ByteArrayBuilder.create(set)).get[mutable.Set[String]])
    }

    {
      val map = Map("a" -> 1, "b" -> 2, "c" -> 3)
      assert(map == ByteArrayReader(ByteArrayBuilder.create(map)).get[Map[String,Int]])
    }

    {
      val map = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
      assert(map == ByteArrayReader(ByteArrayBuilder.create(map)).get[Map[String,Int]])
    }
  }

  /*
   * TESTING IMPLICITS
   */

  test("add custom implicit binarizer") {
    import Implicits.default._

    val data = Seq(new URL("http://www.test1.com"), new URL("http://www.test2.com"))

    implicit val urlBinarizer = new StreamBinarizer[URL] {
      override def write(out: RichOutputStream, url: URL): Unit = out << url.toString
      override def read(in: RichInputStream): URL = new URL(in.get[String])
    }

    assert(data == ByteArrayReader(ByteArrayBuilder.create(data)).get[Seq[URL]])
  }

  test("override an already imported implicit") {
    import Implicits.default._

    val data = Seq(1, 2, 3, 4, 5)

    implicit val myInt = new StreamBinarizer[Int] {
      override def write(out: RichOutputStream, i: Int): Unit = out << i << i
      override def read(in: RichInputStream): Int = { in.get[Int]; in.get[Int] }
    }

    // 'myInt' override the imported implicit Implicits.default.intSortable.
    // This leads to the fact that implicit chaining (such as to Seq) do not work together with the override ('myInt').
    // One needs to either:
    // 1. define it again (as shown here)
    // 2. avoid importing an implicit for the new type (shown in next test)

    // import Implicits.default.implicitSeq // does not suffice, need to do as follows
    implicit val myIntSeq = Explicits.seq[Int](myInt)

    assert(data == ByteArrayReader(ByteArrayBuilder.create(data)).get[Seq[Int]])
  }

  test("use a new type of implicit in parallel to an already imported one") {
    import Implicits.collections._ // avoid importing any basic data type at all, but only functionality to extend them to collections

    val data = Seq(1, 2, 3, 4, 5)

    implicit val myInt = new StreamBinarizer[Int] {
      import Implicits.default.implicitIntSortable
      override def write(out: RichOutputStream, i: Int): Unit = out << i << i
      override def read(in: RichInputStream): Int = { in.get[Int]; in.get[Int] }
    }

    assert(data == ByteArrayReader(ByteArrayBuilder.create(data)).get[Seq[Int]])
  }

  /*
   * CASE CLASS TESTS
   */

  case class Person(name: String, age: Int)

  object Person {
    implicit val binarizer = new StreamBinarizer[Person] {
      import Implicits.default._
      override def write(out: RichOutputStream, p: Person): Unit = out << p.name << p.age
      override def read(in: RichInputStream): Person = Person(in.get[String](), in.get[Int]())
    }
  }

  case class Room(number: String, people: Seq[Person])

  object Room {
    implicit val binarizer = new StreamBinarizer[Room] {
      import Implicits.default._
      override def write(out: RichOutputStream, r: Room): Unit = out << r.number << r.people
      override def read(in: RichInputStream): Room = Room(in.get[String](), in.get[Seq[Person]]())
    }
  }

  test("case class serialization & deserialization") {
    import Implicits.default._

    val data = Map(
      "A" -> Room("A210", Seq(Person("Anton", 24), Person("Mary", 32))),
      "B" -> Room("B101", Seq()),
      "C" -> Room("C93", Seq(Person("Tom", 20)))
    )

    assert(data == ByteArrayReader(ByteArrayBuilder.create(data)).get[Map[String,Room]])
  }
}
