package kv.io

/**
  * An instance of this trait implements functionality to serialize an object of type <code>T</code> into a sequence
  * of bytes, and to deserialize the object of type <code>T</code> from a sequence of bytes.
  * There are two spirits of serialization/deserialization:
  * The first one represents the object as an array of bytes, which carries additional meta-information about the number
  * of bytes used for serialization.
  * The second one is self-contained in the way that it directly writes to/reads from a stream of bytes,
  * which requires that the serialization/deserialization itself takes care of how many bytes to write/read.
  *
  * Whatever approach better fits your needs for a certain type <code>T</code>, you may choose to extend either
  * [[kv.io.ArrayBinarizer ArrayBinarizer]] or [[kv.io.StreamBinarizer StreamBinarizer]].
  * These traits implement the respective other functionality appropriately.
  *
  * @tparam T type of the Object to be serialized/deserialized
  */
trait Binarizer[T] {
  /**
    * Represents the given object <code>obj</code> as an <code>Array[Byte]</code> in such a way
    * that the object can be recreated from the byte array via <code>fromBytes</code>.
    * @param obj object to be serialized
    * @return serialized byte array representation of <code>obj</code>
    */
  def toBytes(obj: T): Array[Byte]

  /**
    * Creates a new object of type <code>T</code> by deserializing the given <code>Array[Byte]</code>, which
    * was created by <code>toBytes</code>.
    * @param bytes serialized representation of the object to return
    * @return deserialized object as extracted from <code>bytes</code>
    */
  def fromBytes(bytes: Array[Byte]): T

  /**
    * Writes the given object to the output stream in such a way that the object can be recreated via <code>read</code> from
    * a corresponding input stream that is positioned at the first byte written.
    * The written information must be self-recoverable from the stream, without requiring additional knowledge
    * (e.g., on how many bytes were written).
    * @param out stream to be written to
    * @param obj object to be serialized
    */
  def write(out: RichOutputStream, obj: T): Unit

  /**
    * Creates a new object of type <code>T</code> by reading exactly those bytes from the stream that were written
    * to it by the corresponding <code>write</code>.
    * Note that <code>RichInputStream</code> provides a flexible access plus the stricter read access guarantees
    * of <code>StrictInputStream</code>.
    * @param in stream to be read
    * @return deserialized object as extracted by reading the input stream
    */
  def read(in: RichInputStream): T
}

/**
  * A subclass of [[kv.io.Binarizer Binarizer]] that requires to implement
  * [[kv.io.Binarizer#toBytes Binarizer.toBytes]] and [[kv.io.Binarizer#fromBytes Binarizer.fromBytes]].
  * This subclass is well-suited for data that naturally serializes to/deserializes from <code>Array[Byte]</code>,
  * for example <code>String</String> via <code>getBytes</code> and <code>String(Array[Byte])</code>, respectively.
  * The pre-implemented streaming functions ([[kv.io.Binarizer#write Binarizer.write]] and
  * [[kv.io.Binarizer#read Binarizer.read]]) write/read the size of the array previous to its content.
  */
trait ArrayBinarizer[T] extends Binarizer[T] {
  def toBytes(o: T): Array[Byte]
  def fromBytes(bytes: Array[Byte]): T

  def write(out: RichOutputStream, o: T): Unit = Explicits.byteArray.write(out, toBytes(o))
  def read(in: RichInputStream): T = fromBytes(Explicits.byteArray.read(in))
}

/**
  * A subclass of [[kv.io.Binarizer Binarizer]] that requires to implement
  * [[kv.io.Binarizer#write Binarizer.write]] and [[kv.io.Binarizer#read Binarizer.read]].
  * This subclass is well-suited for data that can be serialized/deserialized in a self-contained way.
  * Self-contained means that either no additional meta-information is required during deserialization (e.g., the
  * number of bytes written is known to be fixed), or that any required meta-information is manually written/read
  * additionally to the actual content.
  * The pre-implemented byte arrays ([[kv.io.Binarizer#toBytes Binarizer.toBytes]] and
  * [[kv.io.Binarizer#fromBytes Binarizer.fromBytes]]) create/read their content from the stream.
  */
trait StreamBinarizer[T] extends Binarizer[T] {
  def toBytes(o: T): Array[Byte] = {
    val out = ByteArrayBuilder()
    write(out, o)
    out.result()
  }
  def fromBytes(bytes: Array[Byte]): T = read(ByteArrayReader(bytes))

  def write(out: RichOutputStream, o: T): Unit
  def read(in: RichInputStream): T
}