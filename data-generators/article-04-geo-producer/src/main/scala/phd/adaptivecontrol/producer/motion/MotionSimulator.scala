package phd.adaptivecontrol.producer.motion

import scala.util.Random
import phd.adaptivecontrol.producer.model.ObjectState
import phd.adaptivecontrol.producer.util.GeoUtils


object MotionSimulator {

  def move(
    state: ObjectState,
    currentTimestamp: Long,
    rand: Random,
    mode: MotionMode
  ): ObjectState = {

    val dtSeconds =
      ((currentTimestamp - state.lastTimestamp).max(1L)).toDouble / 1000.0

    val updatedSpeed =
      updateSpeed(state.speed, rand, mode)

    val updatedHeading =
      updateHeading(state.heading, rand, mode)

    val distanceMeters =
      updatedSpeed * dtSeconds

    val (newLon, newLat) =
      GeoUtils.moveCoordinate(
        lon = state.lon,
        lat = state.lat,
        headingDegrees = updatedHeading,
        distanceMeters = distanceMeters
      )

    state.copy(
      lon = newLon,
      lat = newLat,
      speed = updatedSpeed,
      heading = updatedHeading,
      lastTimestamp = currentTimestamp
    )
  }


  private def updateSpeed(
    currentSpeed: Double,
    rand: Random,
    mode: MotionMode
  ): Double = {

    mode match {

      case StraightMotion =>
        currentSpeed

      case RandomWalkMotion =>
        (currentSpeed + rand.nextGaussian() * 0.3)
          .max(1.0)
          .min(2.5)

      case SwarmMotion =>
        (currentSpeed + rand.nextGaussian() * 0.15)
          .max(0.7)
          .min(1.5)

      case CollisionMotion =>
        currentSpeed
    }
  }


  private def updateHeading(
    currentHeading: Double,
    rand: Random,
    mode: MotionMode
  ): Double = {

    mode match {

      case StraightMotion =>
        currentHeading

      case RandomWalkMotion =>
        normalizeHeading(
          currentHeading + rand.nextGaussian() * 10.0
        )

      case SwarmMotion =>
        normalizeHeading(
          currentHeading + rand.nextGaussian() * 5.0
        )

      case CollisionMotion =>
        currentHeading
    }
  }


  private def normalizeHeading(heading: Double): Double = {
    ((heading % 360.0) + 360.0) % 360.0
  }
}
