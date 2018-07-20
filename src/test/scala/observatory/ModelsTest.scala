package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalactic.TolerantNumerics

trait ModelsTest extends FunSuite with Checkers {

  val epsilon = 0.001
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  test("distance between equal points") {
    val newYork = Location(40.7128, 74.0060)
    assert(newYork.distance(newYork) == 0.0)
  }

  test("distance between antipodes") {
    val longestDistanceOnEarth = 20037392.104
    val antipodeOne = Location(27.97, -82.53)
    val antipodeTwo = Location(-27.97, 97.47)
    assert(antipodeOne.distance(antipodeTwo) === longestDistanceOnEarth)
  }

  test ("distance between points") {
    val distanceFromNewYorkToLondon = 5576429.773
    val newYork = Location(40.7128, 74.0060)
    val london = Location(51.5074, 0.1278)
    assert(newYork.distance(london) === distanceFromNewYorkToLondon)
  }
}
