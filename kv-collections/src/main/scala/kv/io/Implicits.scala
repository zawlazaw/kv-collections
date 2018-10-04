package kv.io

import scala.reflect.ClassTag
import Explicits._

object Implicits {

  val collections = new Collections {}
  val default = new Default with Collections {}
  val compact = new Compact with Collections {}

  trait Default {
    implicit val implicitBoolean = boolean
    implicit val implicitByte = byte
    implicit val implicitShort = short
    implicit val implicitChar = char
    implicit val implicitIntSortable = intSortable
    implicit val implicitLongSortable = longSortable
    implicit val implicitFloat = float
    implicit val implicitDouble = double
    implicit val implicitString = utf8
    implicit val implicitInstant = instantSortable
  }

  trait Compact {
    implicit val implicitBoolean = boolean
    implicit val implicitByte = byte
    implicit val implicitShort = short
    implicit val implicitChar = char
    implicit val implicitVarlenInt = varlenInt
    implicit val implicitVarlen = varlen
    implicit val implicitFloat = float
    implicit val implicitDouble = double
    implicit val implicitString = utf8
    implicit val implicitInstant = instantSortable
  }

  trait Collections {
    implicit def implicitOption[T](implicit binarizer: Binarizer[T]) = option[T]
    implicit def implicitSome[T](implicit binarizer: Binarizer[T]) = some[T]
    implicit val implicitByteArray = byteArray
    implicit def implicitArray[T:ClassTag](implicit binarizer: Binarizer[T]) = array[T]
    implicit def implicitSeq[T](implicit binarizer: Binarizer[T]) = seq[T]
    implicit def implicitSet[T](implicit binarizer: Binarizer[T]) = set[T]
    implicit def implicitMutableSet[T](implicit binarizer: Binarizer[T]) = mutableSet[T]
    implicit def implicitMap[K,V](implicit keyBinarizer: Binarizer[K], valueBinarizer: Binarizer[V]) = map[K,V]
    implicit def implicitMutableMap[K,V](implicit keyBinarizer: Binarizer[K], valueBinarizer: Binarizer[V]) = mutableMap[K,V]
    implicit def implicitTuple2[A,B](implicit a: Binarizer[A], b: Binarizer[B]) = tuple2[A,B]
    implicit def implicitTuple3[A,B,C](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C]) = tuple3[A,B,C]
    implicit def implicitTuple4[A,B,C,D](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D]) = tuple4[A,B,C,D]
    implicit def implicitTuple5[A,B,C,D,E](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                           e: Binarizer[E]) = tuple5[A,B,C,D,E]
    implicit def implicitTuple6[A,B,C,D,E,F](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                             e: Binarizer[E], f: Binarizer[F]) = tuple6[A,B,C,D,E,F]
    implicit def implicitTuple7[A,B,C,D,E,F,G](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                               e: Binarizer[E], f: Binarizer[F], g: Binarizer[G]) = tuple7[A,B,C,D,E,F,G]
    implicit def implicitTuple8[A,B,C,D,E,F,G,H](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                                 e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H]) = tuple8[A,B,C,D,E,F,G,H]
    implicit def implicitTuple9[A,B,C,D,E,F,G,H,I](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                                   e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                                   i: Binarizer[I]) = tuple9[A,B,C,D,E,F,G,H,I]
    implicit def implicitTuple10[A,B,C,D,E,F,G,H,I,J](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                                      e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                                      i: Binarizer[I], j: Binarizer[J]) = tuple10[A,B,C,D,E,F,G,H,I,J]
    implicit def implicitTuple11[A,B,C,D,E,F,G,H,I,J,K](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                                        e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                                        i: Binarizer[I], j: Binarizer[J], k: Binarizer[K]) = tuple11[A,B,C,D,E,F,G,H,I,J,K]
    implicit def implicitTuple12[A,B,C,D,E,F,G,H,I,J,K,L](implicit a: Binarizer[A], b: Binarizer[B], c: Binarizer[C], d: Binarizer[D],
                                                          e: Binarizer[E], f: Binarizer[F], g: Binarizer[G], h: Binarizer[H],
                                                          i: Binarizer[I], j: Binarizer[J], k: Binarizer[K], l: Binarizer[L]) = tuple12[A,B,C,D,E,F,G,H,I,J,K,L]
  }
}
