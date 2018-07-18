package observatory

import org.scalatest.FunSuite
import java.time.LocalDate

import observatory.Extraction.{StationRecord, TemperatureRecord}
import org.apache.spark.sql.SparkSession

trait ExtractionTest extends FunSuite {

  test("location yearly average records degenerate case") {
    assert(Iterable.empty == Extraction.locationYearlyAverageRecords(Iterable.empty))
  }

  test("location average records") {
    val records = List((LocalDate.of(2010, 1, 2), Location(1.0, 2.0), 5.0), (LocalDate.of(2010, 3, 4), Location(1.0, 2.0), 10.0),
      (LocalDate.of(2010, 2, 3), Location(10.0, 15.0), 7.0), (LocalDate.of(2010, 5, 6), Location(10.0, 15.0), 9.0))
    val actual = Extraction.locationYearlyAverageRecords(records)
    val expected = List((Location(1.0, 2.0), 7.5), (Location(10.0, 15.0), 8.0))
    assert(expected.toSet == actual.toSet)
  }

  test("locate temperature from records degenerate case") {
    val spark = SparkSession.builder.appName("observatory").master("local").getOrCreate()
    import spark.implicits._

    val actual = Extraction.locateTemperaturesFromRecords(
      (List() : List[StationRecord]).toDS,
      (List() : List[TemperatureRecord]).toDS
    )

    assert(actual.collect.isEmpty)
  }

  test("locate temperature from records") {
    val spark = SparkSession.builder.appName("observatory").master("local").getOrCreate()
    import spark.implicits._

    val actual = Extraction.locateTemperaturesFromRecords(
      List(Extraction.StationRecord(1, None, 5, 7)).toDS,
      List(
        Extraction.TemperatureRecord(1, None, 1, 2, 5.0),
        Extraction.TemperatureRecord(1, None, 1, 2, 3.0)
      ).toDS
    )

    assert(actual.collect.toSet == Set((1, 2, Location(5.0, 7.0), 5.0), (1, 2, Location(5.0, 7.0), 3.0)))
  }

  test("ignore invalid temperatures") {
    val spark = SparkSession.builder.appName("observatory").master("local").getOrCreate()
    import spark.implicits._

    val actual = Extraction.locateTemperaturesFromRecords(
      List(Extraction.StationRecord(1, None, 5, 7)).toDS,
      List(
        Extraction.TemperatureRecord(1, None, 1, 2, 5.0),
        Extraction.TemperatureRecord(1, None, 1, 2, 9999.9)
      ).toDS
    )

    assert(actual.collect.deep == Array((1, 2, Location(5.0, 7.0), 5.0)).deep)
  }

  test("integration") {
    assert(Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv") != null)
  }
}
