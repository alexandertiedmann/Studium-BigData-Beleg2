package loganalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.OffsetDateTime

object LogAnalyseFuns {

  def getParsedData(data: RDD[String]): (RDD[(Row, Int)], RDD[Row], RDD[Row]) = {

    val parsed_logs = data.map(Utilities.parse_line(_))
    val access_logs = parsed_logs.filter(_._2 == 1).map(_._1).cache()
    val failed_logs = parsed_logs.filter(_._2 == 0).map(_._1)
    (parsed_logs, access_logs, failed_logs)
  }

  /*
   * Calculate for the content size the following values:
   *
   * minimum: Minimum value
   * maximum: Maximum value
   * average: Average
   *
   * Return the following triple:
   * (min,max,avg)
   */
  def calculateLogStatistic(data: RDD[Row]): (Long, Long, Long) = {
    val size = data.map(f => f.getInt(8)) //aus der Row die Content Size
    (
      size.min(), //Minimum berechnen
      size.max(), //Maximum berechnen
      size.sum().toLong / size.count() //Durschnitt berechnen
    )
  }

  /*
   * Calculate for each single response code the number of occurences
   * Return a list of tuples which contain the response code as the first
   * element and the number of occurences as the second.
   *    *
   */
  def getResponseCodesAndFrequencies(data: RDD[Row]): List[(Int, Int)] = {
    data.groupBy(f => f.getInt(7)) //aus der Row den Response Code gruppieren
      .map { //eine map mit den codes und der anzahl der responses
      case (code, responses) => (code, responses.count(_ => true))
    }
      .collect() //gibt die map als dataset zurück
      .toList //in eine Liste
  }

  /*
   * Calculate 20 arbitrary hosts from which the web server was accessed more than 10 times
   * Print out the result on the console (no tests take place)
   */
  def get20HostsAccessedMoreThan10Times(data: RDD[Row]): List[String] = {
    data.groupBy(f => f.getString(0)) //alle Hosts aus der Row und gruppieren
      .map[(String, Int)] { //eine map mit dem Namen und der Anzahl
      //wenn host name in anderer Zeile dann counten
      case (host, it: Iterable[Row]) => (host, it.count(_ => true))
    }
      .filter { case (_, count) => count > 20 } //nach Hosts mit Count 20 filtern
      .map { case (host, _) => host } //diese in eine neue map
      .take(20) //aus der map 20 herausnehmen und in ein Array schreiben
      .toList //das array in eine Liste umwandeln
  }

  /*
   * Calcuclate the top ten endpoints.
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of accesses as the second
   * The list should be ordered by the number of accesses.
   */
  def getTopTenEndpoints(data: RDD[Row]): List[(String, Int)] = {
    data.groupBy(f => f.getString(5)) //alle angefragten Endpoints
      .map { //eine map mit Name des Endpoints und Anzahl der Anfragen
      case (endpoint, responses) => (endpoint, responses.count(_ => true))
    }
      .sortBy(x => x._2, ascending = false) //sortieren
      .take(10) //die ersten 10 herausnehmen
      .toList //in eine Liste
  }

  /*
   * Calculate the top ten endpoint that produces error response codes (response code != 200).
   *
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of errors as the second.
   * The list should be ordered by the number of accesses.
   */
  def getTopTenErrorEndpoints(data: RDD[Row]): List[(String, Int)] = {
    //alle angefragten ResponseCodes die nicht 200 sind rausfiltern
    data.filter(f => f.getInt(7) != 200)
      .groupBy(f => f.getString(5)) // mit den Endpoints gruppieren
      .map { //eine map mit Name des Endpoints und den Fehlgeschlagenen Anfragen
      row => (row._1, row._2.count(i => true))
    }
      .sortBy(x => x._2, ascending = false) //sortieren
      .take(10) //die ersten 10 herausnehmen
      .toList //in eine Liste
  }


  /*
   * Calculate the number of requests per day.
   * Return a list of tuples which contain the day (1..30) as the first element and the number of
   * accesses as the second.
   * The list should be ordered by the day number.
   */
  def getNumberOfRequestsPerDay(data: RDD[Row]): List[(Int, Int)] = {
    data.groupBy(f => f.getAs[OffsetDateTime](3).getDayOfMonth) //alle Tage bekommen
      .map { //eine map mit Tag und Anzahl der Anzahl der Eingaenge
      case (day, accesses) => (day, accesses.count(_ => true))
    }
      .sortBy(x => x._1, ascending = true) //sortieren
      .collect //in ein Dataset
      .toList //in eine Liste
  }


  /*
   * Calculate the number of hosts that accesses the web server in June 95.
   * Every hosts should only be counted once.
   */
  def numberOfUniqueHosts(data: RDD[Row]): Long = {
    data.map(f => f.getString(0)) //alle Hosts in eine Map
      .distinct() //Doppelte eliminieren
      .count() //ergebnis zaehlen
  }


  /*
  * Calculate the number of hosts per day that accesses the web server.
  * Every host should only be counted once per day.
  * Order the list by the day number.
  */
  def numberOfUniqueDailyHosts(data: RDD[Row]): List[(Int, Int)] = {
    data.groupBy(f => f.getAs[OffsetDateTime](3).getDayOfMonth) //alle Tage bekommen
      .map { //eine map mit Tag und Anzahl den Hosts
      case (day, hosts) =>
        (day, hosts.map { //fuer jeden Tag einen Eintrag in der Map
          hosts => hosts.getString(0) // und die Hosts zuordnen
        }
          .toList //map in eine Liste
          .distinct //doppelte Hosts eliminieren
          .count(_ => true) //uebrige Hosts zaehlen
        )
    }
      .sortBy(x => x._1, ascending = true) //sortieren
      .collect //in ein Dataset
      .toList //in eine Liste
  }


  /*
   * Calculate the average number of requests per host for each single day.
   * Order the list by the day number.
   */
  def averageNrOfDailyRequestsPerHost(data: RDD[Row]): List[(Int, Int)] = {
    data.groupBy(f => f.getAs[OffsetDateTime](3).getDayOfMonth) // Alle Tage des Monats
      .mapValues { // MapValues betrifft nur Values in einer Map, Keys bleiben so
        rows =>
          rows.size / // Die gesamte Size der Row
            rows.map(_.getString(0)).toList.distinct.size // Geteilt durch Unique Hosts

      }
      .sortBy(x => x._1, ascending = true) // Sortieren
      .collect() // Dataset
      .toList // zu einer Liste
  }


  /*
   * Calculate the top 25 hosts that causes error codes (Response Code=404)
   * Return a set of tuples consisting the hostnames  and the number of requests
   */
  def top25ErrorCodeResponseHosts(data: RDD[Row]): Set[(String, Int)] = {
    data.groupBy(f => f.getString(0)) // Alle Hosts bekommen
      .mapValues {
        rows => rows.filter(x => x.getInt(7) == 404).size // Zaehle alle Response Codes 404
      }.sortBy(x => x._2, ascending = false) //  absteigend sortieren, bevor es zum Set wird, Set ist unsortiert
      .take(25) // Nimm 25 Elemente raus
      .toSet // Jetzt zum Set
  }


  /*
   * Calculate the number of error codes (Response Code=404) per day.
   * Return a list of tuples that contain the day as the first element and the number as the second.
   * Order the list by the day number.
   */
  def responseErrorCodesPerDay(data: RDD[Row]): List[(Int, Int)] = {
    data.groupBy(f => f.getAs[OffsetDateTime](3).getDayOfMonth) // Alle Tage des Monats holen
      .mapValues {
        rows => rows.filter(x => x.getInt(7) == 404).size // Zaehle alle Response Codes 404
      }.sortBy(x => x._1, ascending = true) // sortieren, diesmal aufsteigend, nach Tageszahl
      .collect() // Dataset
      .toList // Zu einer Liste
  }


  /*
   * Calculate the error response coded for every hour of the day.
   * Return a list of tuples that contain the hour as the first element (0..23) abd the number of error codes as the second.
   * Ergebnis soll eine Liste von Tupeln sein, deren erstes Element die Stunde bestimmt (0..23) und
   * Order the list by the hour-number.
   */
  def errorResponseCodeByHour(data: RDD[Row]): List[(Int, Int)] = {
    data.groupBy(f => f.getAs[OffsetDateTime](3).getHour) // Die Stunden des Tages holen
      .mapValues {
        rows => rows.filter(x => x.getInt(7) == 404).size // Zaehle alle Response Codes 404
      }.sortBy(x => x._1, ascending = true) // sortieren, diesmal aufsteigend, nach Stunde
      .collect() // Dataset
      .toList // Zu einer Liste
  }


  /*
   * Calculate the number of requests per weekday (Monday, Tuesday,...).
   * Return a list of tuples that contain the number of requests as the first element and the weekday
   * (String) as the second.
   * The elements should have the following order: [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday].
   */
  def getAvgRequestsPerWeekDay(data: RDD[Row]): List[(Int, String)] = {
    data.groupBy(f => f.getAs[OffsetDateTime](3).getDayOfWeek.getValue) // Wochentag, hier als Int erstmal
      .mapValues {
        rows =>
        rows.size / // Die gesamte Row-Size, eine Zeile im Log ist ja ein Request
          rows.map(_.getAs[OffsetDateTime](3).toLocalDate).toList.distinct.size // Hier zaehlen, wie oft ein Wochentag
        // vorkommt, so wird gezaehlt, wie oft Requests DURCHSCHNITTLICH an einem Wochentag stattfinden.
      }
      .map{
      x => (x._2,x._1) // Tupel umdrehen: Von Wochentag, Requests zu -> Requests,Wochentag
    }
      .sortBy(x=>x._2,ascending = true) // Nach Wochentagen sortieren, Wochentage sind hier immer noch nur Zahlen
      // 1 = Montag, 2 = Dienstag usw.
      .mapValues{ // MapValues betrifft nur die Values, Values sind hier die Wochentage, Anzahl der Requests ist der Key
        x => x  match { // Hier die Wochentage zu einem String umwandeln
          case 1 => "Monday"
          case 2 => "Tuesday"
          case 3 => "Wednesday"
          case 4 => "Thursday"
          case 5 => "Friday"
          case 6 => "Saturday"
          case 7 => "Sunday"
        }
      }
      .collect() // Dataset
      .toList // zu einer Liste
    }
}
