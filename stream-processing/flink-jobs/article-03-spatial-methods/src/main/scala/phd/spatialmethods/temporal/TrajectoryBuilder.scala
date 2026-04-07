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
        Trajectory(
          objectId = event.objectId,
          events = Seq(event)
        )

      case Some(traj) =>
        val lastEventOpt = traj.sortedEvents.lastOption

        lastEventOpt match {

          case Some(lastEvent) =>
            val timeGap =
              event.timestamp.toEpochMilli - lastEvent.timestamp.toEpochMilli

            if (timeGap <= maxGap.toMillis && timeGap >= 0) {
              // Continue trajectory
              traj.addEvent(event)
            } else {
              // Gap too large OR out-of-order → start new trajectory
              Trajectory(
                objectId = event.objectId,
                events = Seq(event)
              )
            }

          case None =>
            // Edge case: empty trajectory
            Trajectory(
              objectId = event.objectId,
              events = Seq(event)
            )
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

    Trajectory(
      objectId = t1.objectId,
      events = (t1.events ++ t2.events)
        .sortBy(_.timestamp.toEpochMilli)
    )
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
          currentSegment = List(next)
        }
    }

    segments += currentSegment

    segments.map(events =>
      Trajectory(traj.objectId, events)
    ).toSeq
  }

}
