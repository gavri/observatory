package observatory

import java.time.LocalDate

import org.apache.spark.sql.{Dataset, SparkSession, types}

/**
  * 1st milestone: data extraction
  */
object Extraction {

  case class StationRecord(stn: Int, wban: Option[Int], latitude: Double, longitude: Double) {
    def location = {
      Location(latitude, longitude)
    }
  }

  case class TemperatureRecord(stn: Int, wban: Option[Int], month: Int, day: Int, temperature: Temperature)

  val spark = SparkSession.builder.appName("observatory").master("local").getOrCreate()
  import spark.implicits._

  val missingTemperatureMarker = 9999.9

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsFilePath = this.getClass.getResource(stationsFile).toURI.toString
    val stationColumns = List("stn", "wban", "latitude", "longitude")
    val stations = spark.read.option("inferSchema", true).
      schema(types.StructType(Seq(
        types.StructField("stn", types.IntegerType, false),
        types.StructField("wban", types.IntegerType, true),
        types.StructField("latitude", types.DoubleType, true),
        types.StructField("longitude", types.DoubleType, true)))).
    csv(stationsFilePath).toDF(stationColumns: _*).
      na.drop(Seq("latitude", "longitude")).
      as[StationRecord]
    val temperatureColumns = List("stn", "wban", "month", "day", "temperature")
    val temperaturesFilePath = this.getClass.getResource(temperaturesFile).toURI.toString
    val temperatures = spark.read.option("inferSchema", true).csv(temperaturesFilePath).toDF(temperatureColumns: _*).as[TemperatureRecord]
    locateTemperaturesFromRecords(stations, temperatures).collect.map { case(month, day, location, temperature) =>
      (LocalDate.of(year, month, day), location, temperature)
    }
  }


  def locateTemperaturesFromRecords(stations: Dataset[StationRecord], allTemperatures: Dataset[TemperatureRecord]): Dataset[(Int, Int, Location, Temperature)] = {
    val temperatures = allTemperatures.where(s"temperature != $missingTemperatureMarker")
    val stnEquality = stations("stn") === temperatures("stn")
    val wbanEquality = stations("wban") <=> temperatures("wban")
    val joined: Dataset[(StationRecord, TemperatureRecord)] = stations.joinWith(temperatures, stnEquality && wbanEquality)
    joined.map { (record: (StationRecord, TemperatureRecord)) =>
      val stationRecord = record._1
      val temperatureRecord = record._2
      val temperature = temperatureRecord.temperature
      (temperatureRecord.month, temperatureRecord.day, stationRecord.location, temperature)
    }
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val resultAsMap = records.groupBy(_._2).mapValues { records =>
      val temperatures = records.map(_._3)
      temperatures.sum / temperatures.size
    }
    resultAsMap.toList
  }

}
