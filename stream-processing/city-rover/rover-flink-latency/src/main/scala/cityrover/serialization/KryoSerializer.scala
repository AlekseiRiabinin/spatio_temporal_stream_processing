package cityrover.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.{Input, Output}


class KryoSerializer[T](using ser: Serializer[T]) extends Serializer[T] {

  override def write(kryo: Kryo, output: Output, value: T): Unit =
    ser.write(kryo, output, value)

  override def read(kryo: Kryo, input: Input, tpe: Class[_ <: T]): T =
    ser.read(kryo, input, tpe)
}

object KryoSerializer {

  inline def apply[T](using s: Serializer[T]): KryoSerializer[T] =
    new KryoSerializer[T]
}
