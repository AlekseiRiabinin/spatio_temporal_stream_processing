package phd.architecture.metrics

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.{IHTTPSession, Response}


class MetricsHttpServer(port: Int) extends NanoHTTPD(port) {

  override def serve(session: IHTTPSession): Response = {
    val json = MetricsRegistry.toJson
    NanoHTTPD.newFixedLengthResponse(
      Response.Status.OK,
      "application/json",
      json
    )
  }
}

object MetricsHttpServer {
  def start(port: Int = 9000): Unit = {
    val server = new MetricsHttpServer(port)
    server.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false)
    println(s"[metrics] HTTP server started on port $port")
  }
}
