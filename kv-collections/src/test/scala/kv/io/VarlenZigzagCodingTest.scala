package kv.io

import org.scalatest.FunSuite
import VarlenZigzagCoding._

class VarlenZigzagCodingTest extends FunSuite {

  private val intValues = Seq[Int](0, 1, -1, 1000, -1000, 100000, -100000, 1000000000, -1000000000, Int.MinValue, Int.MaxValue)
  private val longValues = Seq[Long](0, 1, -1, 1000, -1000, 1000000000L, -1000000000L, 1000000000000000000L, -1000000000000000000L, Long.MinValue, Long.MaxValue)

  test("variable length encoding and zig zag encoding") {

    for (l <- longValues) {
      val zz = toVarlenZizag(l)
      val ll = readVarlenZigzag(zz)
      assert(l == ll)

      if (l >= 0) {
        val vl = toVarlen(l)
        val ll = readVarlen(vl)
        assert(l == ll)
      }
    }

    for (l <- intValues) {
      val zz = toVarlenZizag(l)
      val ll = readVarlenZigzag(zz).toInt
      assert(l == ll)

      if (l >= 0) {
        val vl = toVarlen(l)
        val ll = readVarlen(vl).toInt
        assert(l == ll)
      }
    }
  }
}