package observatory

import com.sksamuel.scrimage.{Image, Pixel}

/**
  * 2nd milestone: basic visualization
  */
object Visualization {

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val neighborTemperature = temperatures.find { case (otherLocation, temperature) =>
      location.isNeighborOf(otherLocation)
    }.map(_._2)
    neighborTemperature getOrElse {
      val proximitesWithTemperatures = temperatures.map { case (otherLocation, temperature) =>
        val proximity = location.proximity(otherLocation)
        (proximity, temperature)
      }
      val numerator = proximitesWithTemperatures.map(v => v._1 * v._2).sum
      val denominator = proximitesWithTemperatures.map(_._1).sum
      numerator / denominator
    }
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    ???
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    ???
  }

}

