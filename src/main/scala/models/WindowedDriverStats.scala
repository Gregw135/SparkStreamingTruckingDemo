package models

/**
  * @author Edgar Orendain <edgar@orendainx.com>
  */
case class WindowedDriverStats(driverId: Int, averageSpeed: Float, fog: Float, rain: Float, wind: Float, totalViolations: Int, totalEvents: Int) {
  lazy val toCSV = s"$driverId|$averageSpeed|$fog|$rain|$wind|$totalViolations|$totalEvents"
}
