package phd.adaptivecontrol.producer.motion


sealed trait MotionMode {
  def name: String
}


case object StraightMotion extends MotionMode {
  override val name: String = "straight"
}


case object RandomWalkMotion extends MotionMode {
  override val name: String = "random_walk"
}


case object SwarmMotion extends MotionMode {
  override val name: String = "swarm"
}


case object CollisionMotion extends MotionMode {
  override val name: String = "collision"
}


object MotionMode {

  def fromEnv(): MotionMode = {

    sys.env
      .getOrElse("MOTION_MODE", "straight")
      .toLowerCase match {

      case "straight" =>
        StraightMotion

      case "random_walk" | "randomwalk" =>
        RandomWalkMotion

      case "swarm" =>
        SwarmMotion

      case "collision" =>
        CollisionMotion

      case other =>
        println(
          s"[MotionMode] Unknown mode '$other', using straight"
        )
        StraightMotion
    }
  }
}
