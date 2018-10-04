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
  }
}
