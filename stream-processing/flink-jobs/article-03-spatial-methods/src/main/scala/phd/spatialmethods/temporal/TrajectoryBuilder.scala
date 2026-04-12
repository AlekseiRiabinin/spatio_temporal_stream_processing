package phd.spatialmethods.temporal

import scala.collection.mutable
import phd.spatialmethods.model.{GeoEvent, Trajectory}


/**
 * TrajectoryBuilder incrementally constructs trajectories
 * from incoming GeoEvents grouped by objectId.
 *
 * Stateless version compatible with epoch-millis time model.
 */
class TrajectoryBuilder(
  maxGapMs: Long // max allowed gap between events (milliseconds)
) {

  /**
   * Update trajectory with a new incoming event.
   */
  def updateTrajectory(
    current: Option[Trajectory],
    event: GeoEvent
  ): Trajectory = {

    current match {

      case None =>
        val newTraj = Trajectory(
          objectId = event.objectId,
          events = Seq(event)
        )

        println(
          s"[TRAJECTORY] start objectId=${event.objectId} events=1 ts=${event.timestamp}"
        )

        newTraj

      case Some(traj) =>
        val lastEventOpt = traj.sortedEvents.lastOption

        lastEventOpt match {

          case Some(lastEvent) =>
            val timeGap = event.timestamp - lastEvent.timestamp

            if (timeGap <= maxGapMs && timeGap >= 0) {

              val updated = traj.addEvent(event)

              println(
                s"[TRAJECTORY] update objectId=${event.objectId} " +
                s"length=${updated.events.size} " +
                s"timeGap=$timeGap lastTs=${lastEvent.timestamp} newTs=${event.timestamp}"
              )

              updated

            } else {

              println(
                s"[TRAJECTORY] reset objectId=${event.objectId} " +
                s"reason=gap_exceeded gap=$timeGap maxGap=$maxGapMs " +
                s"lastTs=${lastEvent.timestamp} newTs=${event.timestamp}"
              )

              Trajectory(
                objectId = event.objectId,
                events = Seq(event)
              )
            }

          case None =>
            val newTraj = Trajectory(
              objectId = event.objectId,
              events = Seq(event)
            )

            println(
              s"[TRAJECTORY] start-empty objectId=${event.objectId} events=1 ts=${event.timestamp}"
            )

            newTraj
        }
    }
  }

  /**
   * Merge trajectories (same object only)
   */
  def mergeTrajectories(
    t1: Trajectory,
    t2: Trajectory
  ): Trajectory = {

    require(
      t1.objectId == t2.objectId,
      "Cannot merge trajectories of different objects"
    )

    val merged = Trajectory(
      objectId = t1.objectId,
      events = (t1.events ++ t2.events)
        .sortBy(_.timestamp)
    )

    println(
      s"[TRAJECTORY] merge objectId=${t1.objectId} " +
      s"len1=${t1.events.size} len2=${t2.events.size} merged=${merged.events.size}"
    )

    merged
  }

  /**
   * Split trajectory into segments based on time gaps
   */
  def segmentTrajectory(traj: Trajectory): Seq[Trajectory] = {

    val sorted = traj.sortedEvents
    if (sorted.isEmpty) return Seq.empty

    val segments = mutable.ListBuffer[List[GeoEvent]]()
    var currentSegment = List(sorted.head)

    sorted.sliding(2).foreach {
      case Seq(prev, next) =>
        val gap = next.timestamp - prev.timestamp

        if (gap <= maxGapMs) {
          currentSegment = currentSegment :+ next
        } else {
          segments += currentSegment

          println(
            s"[TRAJECTORY] segment objectId=${traj.objectId} " +
            s"segmentSize=${currentSegment.size} gap=$gap"
          )

          currentSegment = List(next)
        }
    }

    segments += currentSegment

    println(
      s"[TRAJECTORY] segment-final objectId=${traj.objectId} " +
      s"segmentSize=${currentSegment.size}"
    )

    segments.map(events =>
      Trajectory(traj.objectId, events)
    ).toSeq
  }
}
