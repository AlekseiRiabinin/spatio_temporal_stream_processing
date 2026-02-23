package phd.streammodels.model


sealed trait StreamModelType

object StreamModelType {
  case object Actor extends StreamModelType
  case object Log extends StreamModelType
  case object Dataflow extends StreamModelType
  case object MicroBatch extends StreamModelType
}
