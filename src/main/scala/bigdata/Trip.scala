package bigdata

case class Trip(start_date: String, start_station_code: Int, end_date: String, end_station_code: Int,
                duration_sec: Int, is_member: Int)
object Trip {
  def apply(csv: String): Trip = {
    val f = csv.split(",", -1)
    Trip(f(0), f(1).toInt, f(2), f(3).toInt, f(4).toInt, f(5).toInt)
  }
}
