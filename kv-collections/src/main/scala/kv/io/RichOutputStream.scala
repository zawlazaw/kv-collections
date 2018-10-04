package kv.io

import java.io.OutputStream

trait RichOutputStream extends OutputStream {

  def downstream: OutputStream

  /*
   * OutputStream overrides
   */

  override def write(i: Int): Unit = downstream.write(i)
  override def write(array: Array[Byte]): Unit = downstream.write(array) // forward to OutputStream so that it may provide write optimization
  override def write(array: Array[Byte], off: Int, len: Int): Unit = downstream.write(array, off, len) // forward to OutputStream so that it may provide write optimization
  override def flush(): Unit = downstream.flush()
  override def close(): Unit = downstream.close()

  /*
   * Implicit writes for any kind of data (for which an implicit binarizer is in scope)
   */

  def append[T](obj: T)(implicit binarizer: Binarizer[T]): this.type = { binarizer.write(this, obj); this }
  def << [T](obj: T)(implicit binarizer: Binarizer[T]): this.type = { binarizer.write(this, obj); this }
}

object RichOutputStream {
  def apply(stream: OutputStream): RichOutputStream = new RichOutputStream { val downstream: OutputStream = stream }
}
