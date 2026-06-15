package phd.adaptivecontrol.config


sealed trait StrategyMode

object StrategyMode {
  case object Fixed extends StrategyMode
  case object Adaptive extends StrategyMode
}
