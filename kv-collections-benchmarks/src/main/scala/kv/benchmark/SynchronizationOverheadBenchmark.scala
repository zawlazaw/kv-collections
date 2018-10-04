package kv.benchmark

import scala.collection.mutable
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object SynchronizationOverheadBenchmark extends Bench.LocalTime {

  /*
   * HELPER TRAITS & CLASSES
   */

  trait Counter {
    var count: Long
    def inc(): Unit
  }

  class Synced extends Counter {
    var count = 0L
    def inc(): Unit = synchronized { count += 1 }
  }

  class Unsynced extends Counter {
    var count = 0L
    def inc(): Unit = count += 1
  }

  /*
   * BENCHMARK CONFIGURATION
   */

  val implementations = mutable.LinkedHashMap(
    ("synchronized", () => new Synced),
    ("unsynchronized", () => new Unsynced)
  )

  val names: Gen[String] = Gen.enumeration("name")(implementations.keys.toSeq:_*)
  val ns: Gen[Int] = Gen.exponential("n")(100, 100000000, 100)

  /*
   * BENCHMARK EXECUTION
   */

  performance of s"synchronized vs. unsynchronized counting" in {
    using(Gen.crossProduct(ns, names)) in { case (n, name) => {
      val counter = implementations(name)()
      for (_ <- 1 to n) {
        counter.inc()
      }
    }}
  }
}