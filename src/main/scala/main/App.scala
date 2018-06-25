package main


// Start mit: env JAVA_OPTS="-Xmx4g" sbt run

/**
 * @author hendrik
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.time.OffsetDateTime
import loganalyse._
import org.jfree.ui.ApplicationFrame;

object App extends App {
 
  println("Analyse des NASA-Logfiles vom Juli 1995")
  val conf= new SparkConf().setMaster("local[4]").setAppName("Beleg2")
   conf.set("spark.executor.memory","4g")
   conf.set("spark.storage.memoryFraction","0.8")
  conf.set("spark.driver.memory", "2g")
  val sc= new SparkContext(conf)
  
  val logs=Utilities.getData("NASA_access_log_Jul95.txt","resources",sc)
  val (parsed_logs, access_logs, failed_logs)= LogAnalyseFuns.getParsedData(logs)
  
  println("Stastitik")
  println("Anzahl der geparsten Datensätze:"+access_logs.count)
  println("Anzahl der fehlerhaften Datensätze:"+failed_logs.count)
  println("Die ersten 10 fehlerhaften Datensätze:")
  failed_logs.take(10).foreach(println)
  
  println("Analyse der relativen Häufigkeiten der Response Codes")
  val responseCodes= LogAnalyseFuns.getResponseCodesAndFrequencies(access_logs)
  val appframe1= Graphs.createPieChart(responseCodes)
  println("Please press enter....")
  System.in.read()
  appframe1.setVisible(false)
  appframe1.dispose
  
  println("Analyse der Requests pro Tag")
  val requestsPerDay= LogAnalyseFuns.getNumberOfRequestsPerDay(access_logs)
  val appframe2= Graphs.createLineChart(requestsPerDay,"Requests Per Day","Tag","Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe2.setVisible(false)
  appframe2.dispose
  
  println("Analyse der Errors pro Tag")
  val errorCodesPerDay= LogAnalyseFuns.responseErrorCodesPerDay(access_logs)
  val appframe3= Graphs.createLineChart(errorCodesPerDay,"Error Codes Per Day","Tag","Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe3.setVisible(false)
  appframe3.dispose
  
  println("Durchschnittliche Anzahl der Requests pro Host und Tag")
  val avgNrOfRequestsPerDayAndHost= LogAnalyseFuns.averageNrOfDailyRequestsPerHost(access_logs)
  val appframe4= Graphs.createLineChart(avgNrOfRequestsPerDayAndHost,"Average Number Of Requests Per Day and Host","Tag","Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe4.setVisible(false)
  appframe4.dispose
  
  println("Durchschnittliche Anzahl der Requests pro Wochentag")
  val avgNrOfRequestsPerWeekDay= LogAnalyseFuns.getAvgRequestsPerWeekDay(access_logs)
  val appframe5= Graphs.createBarChart(avgNrOfRequestsPerWeekDay,"Durchschnittliche Anzahl der Requests pro Wochentag","Tag","Anzahl")
  println("Please press enter....")
  System.in.read()
  appframe5.setVisible(false)
  appframe5.dispose  
  
  sc.stop
}