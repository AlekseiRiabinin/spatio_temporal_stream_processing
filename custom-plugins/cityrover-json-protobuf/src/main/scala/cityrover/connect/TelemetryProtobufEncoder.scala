package cityrover.connect

import cityrover.telemetry.{Telemetry => ProtoTelemetry}


object TelemetryProtobufEncoder {

  def encode(telemetry: TelemetryData): Array[Byte] = {

    val proto = ProtoTelemetry(
      roverId = telemetry.roverId,
      ts = telemetry.ts,
      lat = telemetry.lat,
      lon = telemetry.lon,
      speed = telemetry.speed,
      heading = telemetry.heading,
      edgeId = telemetry.edgeId,
      routeId = telemetry.routeId
    )

    proto.toByteArray
  }
}
