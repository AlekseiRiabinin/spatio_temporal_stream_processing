package phd.spatialmethods.temporal

import java.time.Duration
import scala.collection.mutable
import phd.spatialmethods.model.{GeoEvent, Trajectory}


/**
 * TrajectoryBuilder incrementally constructs trajectories
 * from incoming GeoEvents grouped by objectId.
 *
 * This class simulates stateful stream processing logic.
 */
class TrajectoryBuilder(
  maxGap: Duration // max allowed gap between events (trajectory segmentation)
) {

  /**
   * Update trajectory with a new incoming event.
   *
   * @param current Optional existing trajectory
   * @param event   New incoming GeoEvent
   * @return Updated or new trajectory
   */
  def updateTrajectory(
    current: Option[Trajectory],
    event: GeoEvent
  ): Trajectory = {

    current match {

      case None =>
        // Start new trajectory
        val newTraj = Trajectory(
          objectId = event.objectId,
          events = Seq(event)
        )

        println(
          s"[TRAJECTORY] start objectId=${event.objectId} " +
          s"events=1 timestamp=${event.timestamp.toEpochMilli}"
        )

        newTraj

      case Some(traj) =>
        val lastEventOpt = traj.sortedEvents.lastOption

        lastEventOpt match {

          case Some(lastEvent) =>
            val timeGap =
              event.timestamp.toEpochMilli - lastEvent.timestamp.toEpochMilli

            if (timeGap <= maxGap.toMillis && timeGap >= 0) {

              // Continue trajectory
              val updated = traj.addEvent(event)

              println(
                s"[TRAJECTORY] update objectId=${event.objectId} " +
                s"length=${updated.events.size} " +
                s"timeGap=$timeGap " +
                s"lastTs=${lastEvent.timestamp.toEpochMilli} " +
                s"newTs=${event.timestamp.toEpochMilli}"
              )

              updated

            } else {

              // Gap too large OR out-of-order → start new trajectory
              println(
                s"[TRAJECTORY] reset objectId=${event.objectId} " +
                s"reason=gap_exceeded gap=$timeGap maxGap=${maxGap.toMillis} " +
                s"lastTs=${lastEvent.timestamp.toEpochMilli} " +
                s"newTs=${event.timestamp.toEpochMilli}"
              )

              Trajectory(
                objectId = event.objectId,
                events = Seq(event)
              )
            }

          case None =>
            // Edge case: empty trajectory
            val newTraj = Trajectory(
              objectId = event.objectId,
              events = Seq(event)
            )

            println(
              s"[TRAJECTORY] start-empty objectId=${event.objectId} " +
              s"events=1 timestamp=${event.timestamp.toEpochMilli}"
            )

            newTraj
        }
    }
  }

  /**
   * Merge trajectories (useful for windowed or distributed processing)
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
        .sortBy(_.timestamp.toEpochMilli)
    )

    println(
      s"[TRAJECTORY] merge objectId=${t1.objectId} " +
      s"len1=${t1.events.size} len2=${t2.events.size} " +
      s"merged=${merged.events.size}"
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
        val gap = next.timestamp.toEpochMilli - prev.timestamp.toEpochMilli

        if (gap <= maxGap.toMillis) {
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
