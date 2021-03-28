package bigdata

case class StationsInfo(system_id: String, timezone: String, station_id: Int, name: String,
                        short_name: String, lat: Double, lon: Double, capacity: Int)
object StationsInfo {
  def apply(csv: String): StationsInfo = {
    val f = csv.split(",", -1)
    StationsInfo(f(0), f(1), f(2).toInt, f(3), f(4), f(5).toDouble, f(6).toDouble, f(7).toInt)
  }
}
