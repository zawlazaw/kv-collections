package kv.benchmark

import scala.collection.mutable
import kv.io.{ByteArrayBuilder, RichOutputStream}
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object ByteArrayOutputStreamBenchmark extends Bench.LocalTime {

  /*
   * BENCHMARK CONFIGURATION
   */

  val implementations = mutable.LinkedHashMap(
    ("Java's ByteArrayOutputStream               ", () => new java.io.ByteArrayOutputStream()),
    ("ByteArrayBuilder                           ", () => ByteArrayBuilder()),
    ("RichOutputStream[ByteArrayOutputStream]    ", () => RichOutputStream(new java.io.ByteArrayOutputStream())),
    ("Apache Commons' ByteArrayOutputStream______", () => new org.apache.commons.io.output.ByteArrayOutputStream())
  )

  val names: Gen[String] = Gen.enumeration("name")(implementations.keys.toSeq:_*)
  val sizes: Gen[Int] = Gen.exponential("size")(1, 100000, 10)

  val n = 1000000

  /*
   * BENCHMARK EXECUTION
   */

  performance of s"writing $n bytes, batched into different sizes" in {
    using(Gen.crossProduct(sizes, names)) in { case (size, name) => {
      val factory = implementations(name)
      for (_ <- 1 to n / size) {
        val out = factory()
        for (j <- 1 to size)
          out.write(j)
        out.flush()
      }
    }}
  }
}