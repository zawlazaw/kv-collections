package kv.serialization

import org.scalatest.FunSuite

class DynamicByteArrayTest extends FunSuite {

  test("basic functionality") {
    val buf = DynamicByteArray()
    assert(buf.size == 0)
    assert(buf.capacity == DynamicByteArray.DefaultInitialCapacity)
    assert(buf.toSeq == Seq())
    assert(buf.toIndexedSeq == IndexedSeq())
    assert(buf.toList == IndexedSeq())
    assert(buf.toArray sameElements Array.empty[Byte])

    buf += 'H'
    assert(buf.size == 1)
    assert(buf.toSeq == Seq('H'))
    assert(buf.toIndexedSeq == IndexedSeq('H'))
    assert(buf.toList == IndexedSeq('H'))
    assert(buf.toArray sameElements Array('H'))

    buf ++= Seq('e', 'l', 'l', 'o')
    assert(buf.size == 5)
    assert(buf.toString == "Hello")

    buf.clear()
    assert(buf.size == 0)
    assert(buf.toSeq == Seq())
    assert(buf.toIndexedSeq == IndexedSeq())
    assert(buf.toList == IndexedSeq())
    assert(buf.toArray sameElements Array.empty[Byte])
  }

  test("capacity reset") {
    val initialCapacity = 123
    val buf = DynamicByteArray(initialCapacity)

    assert(buf.size == 0)
    assert(buf.capacity == initialCapacity)

    val n = 100 * initialCapacity
    (1 to n).foreach(buf.write)

    assert(buf.size == n)
    assert(buf.capacity >= n)

    buf.clear()
    assert(buf.size == 0)
    assert(buf.capacity >= n)

    buf.clear(resetToInitialCapacity = true)
    assert(buf.capacity == initialCapacity)
  }

  test("get by reference") {
    val buf = DynamicByteArray(8)
    (1 to 8).foreach(buf.write)
    val array1 = buf.toArray(returnBufferIfFull = true)
    buf.clear()
    (1 to 8).foreach(buf.write)
    val array2 = buf.toArray(returnBufferIfFull = true)
    assert(array1 == array2) // intentionally comparing references
  }
}