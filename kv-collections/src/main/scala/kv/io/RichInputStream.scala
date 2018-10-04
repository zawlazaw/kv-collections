package kv.io

import java.io.InputStream

trait RichInputStream extends StrictInputStream {

  /*
   * Implicit reads for any kind of data (for which an implicit binarizer is in scope)
   */

  def get[T]()(implicit binarizer: Binarizer[T]): T = binarizer.read(this)
}

object RichInputStream {
  def apply(stream: InputStream): RichInputStream = new RichInputStream { val upstream: InputStream = stream }
}
