package observatory


import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalactic.TolerantNumerics

trait VisualizationTest extends FunSuite with Checkers {
  test("predict temperature as equal to neighbors' temperature if neighbor exists") {
    val aStarbucks = Location(40.748293, -73.989311)
    val starbucksNextDoor = Location(40.749358, -73.983711)
    val farAwayPlace = Location(51.5074, 0.1278)
    val temperatures = Seq(
      (farAwayPlace, 5.0),
      (starbucksNextDoor, 90.0)
    )
    assert(Visualization.predictTemperature(temperatures, aStarbucks) == 90.0)
  }

  test("predict temperature by inverse distance weighing if there are no neighbors") {
    val epsilon = 0.001
    implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

    val location = Location(40.0, 70.0)
    val temperatures = Seq(
      (Location(50.0, 80.0), 10.0),
      (Location(30.0, 60.0), 20.0)
    )
    assert(Visualization.predictTemperature(temperatures, location) === 14.728)
  }

}
