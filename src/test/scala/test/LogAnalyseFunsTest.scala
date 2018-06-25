package test

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.Row
import loganalyse._

@RunWith(classOf[JUnitRunner])
class LogAnalyseFunsTest extends FunSuite with BeforeAndAfterAll{
  
  var conf:org.apache.spark.SparkConf=_
  var sc:SparkContext=_
  var parsed_logs:RDD[(Row,Int)]= _
  var access_logs:RDD[Row]= _
  var failed_logs:RDD[Row]= _
  
  override protected def beforeAll() {
    
      conf= new SparkConf().setMaster("local[4]").setAppName("BelegLogAnalyse")
      conf.set("spark.executor.memory","4g")
      conf.set("spark.storage.memoryFraction","0.8")
      conf.set("spark.driver.memory", "2g")
      sc= new SparkContext(conf)
      
      val data= Utilities.getData("apache.access.log.PROJECT","resources",sc)

      val X= LogAnalyseFuns.getParsedData(data)
      parsed_logs =X._1
      access_logs= X._2 
      failed_logs= X._3
  }

  test("Parsing Successful"){

     assert(parsed_logs.count===1043177)
     assert(access_logs.count===1043177)
  }

  test("Parsing Failed Logs"){
      
     val fl= failed_logs.count
     if (fl > 0) {
        println ("Number of invalid loglines: "+fl)
        for (line <- failed_logs.take(20)){
            println ("Invalid logline: "+ line)
        }
     }
     assert(failed_logs.count===0)
  }
    
  test("Statistics"){
     
      val (min,max,avg)= LogAnalyseFuns.calculateLogStatistic(access_logs)

      println("Min:"+ min)
      println("Max:"+ max)
      println("Avg:"+ avg)
      assert(min===0)
      assert(max===3421948)
      assert(avg===17531)
  }
   
  
  test("Analyse Response Codes"){

    val res= LogAnalyseFuns.getResponseCodesAndFrequencies(access_logs).sortWith(((X,Y)=>(X._1 < Y._1)))
    assert(res==List((200, 940847), (302, 16244), (304, 79824), (403, 58), (404, 6185), (500, 2), (501, 17)))
    
  }
 
  test("Hosts Accessed More That 10 times"){
    
    val res= LogAnalyseFuns.get20HostsAccessedMoreThan10Times(access_logs)
    println("Hosts Accessed More Than 10 times: "+res)
    assert(res.length===20)
  }
  
  test("Find top 10 Endpoints"){

    val topEndpoints =List(("/images/NASA-logosmall.gif", 59737), ("/images/KSC-logosmall.gif", 50452), ("/images/MOSAIC-logosmall.gif", 43890), 
        ("/images/USA-logosmall.gif", 43664), ("/images/WORLD-logosmall.gif", 43277), ("/images/ksclogo-medium.gif", 41336), ("/ksc.html", 28582), 
        ("/history/apollo/images/apollo-logo1.gif", 26778), ("/images/launch-logo.gif", 24755), ("/", 20292))
    val res= LogAnalyseFuns.getTopTenEndpoints(access_logs)    
    assert(res===topEndpoints)    
  }
 
  test("Find top 10 Error-Endpoints"){

    val topErrorEndpoints =List(("/images/NASA-logosmall.gif", 8761), ("/images/KSC-logosmall.gif", 7236), ("/images/MOSAIC-logosmall.gif", 5197), 
        ("/images/USA-logosmall.gif", 5157), ("/images/WORLD-logosmall.gif", 5020), ("/images/ksclogo-medium.gif", 4728), ("/history/apollo/images/apollo-logo1.gif", 2907), 
        ("/images/launch-logo.gif", 2811), ("/", 2199), ("/images/ksclogosmall.gif", 1622))
    val res= LogAnalyseFuns.getTopTenErrorEndpoints(access_logs)    
    assert(res===topErrorEndpoints)    
  }
  
  test("Count Request per Day"){

    val requestsPerDay= List((1,33996), (3,41387), (4,59554), (5,31888), (6,32416), (7,57355), (8,60142), (9,60457), (10,61245), (11,61242), (12,38070), (13,36480), 
        (14,59873), (15,58845), (16,56651), (17,58980), (18,56244), (19,32092), (20,32963), (21,55539), (22,57758))
    val res= LogAnalyseFuns.getNumberOfRequestsPerDay(access_logs)
    assert(res===requestsPerDay)
  }
  
  test("Count Unique Hosts"){
    
    val res= LogAnalyseFuns.numberOfUniqueHosts(access_logs)
    assert(res===54507)
  }
  
  test("Number Of Unique Daily Hosts"){
    
    val nrOfUniqueDailyHosts= List((1, 2582), (3, 3222), (4, 4190), (5, 2502), (6, 2537), (7, 4106), (8, 4406), (9, 4317), (10, 4523), (11, 4346), (12, 2864), 
        (13, 2650), (14, 4454), (15, 4214), (16, 4340), (17, 4385), (18, 4168), (19, 2550), (20, 2560), (21, 4134), (22, 4456))
    val res= LogAnalyseFuns.numberOfUniqueDailyHosts(access_logs)
    assert(res===nrOfUniqueDailyHosts)
  }
  
  test("Average Number of requests by Day and Host"){
    
    val avgNrOfRequestPerDayAndHost= List((1, 13), (3, 12), (4, 14), (5, 12), (6, 12), (7, 13), (8, 13), (9, 14), (10, 13), (11, 14), (12, 13), (13, 13), (14, 13), (15, 13), (16, 13), 
        (17, 13), (18, 13), (19, 12), (20, 12), (21, 13), (22, 12))
    val res= LogAnalyseFuns.averageNrOfDailyRequestsPerHost(access_logs)

    assert(avgNrOfRequestPerDayAndHost===res)
  }
  
  test("Top 25 Error response Code hosts"){
  
    val top25ErrorResponseCodeHosts= Set(("maz3.maz.net", 39),("piweba3y.prodigy.com", 39), ("gate.barr.com", 38), ("m38-370-9.mit.edu", 37), ("ts8-1.westwood.ts.ucla.edu", 37), 
        ("nexus.mlckew.edu.au", 37), ("204.62.245.32", 33), ("163.206.104.34", 27), ("spica.sci.isas.ac.jp", 27), ("www-d4.proxy.aol.com", 26), ("www-c4.proxy.aol.com", 25), 
        ("203.13.168.24", 25), ("203.13.168.17", 25), ("internet-gw.watson.ibm.com", 24), ("scooter.pa-x.dec.com", 23), ("crl5.crl.com", 23), ("piweba5y.prodigy.com", 23), 
        ("onramp2-9.onr.com", 22), ("slip145-189.ut.nl.ibm.net", 22), ("198.40.25.102.sap2.artic.edu", 21), ("gn2.getnet.com", 20), ("msp1-16.nas.mr.net", 20), 
        ("isou24.vilspa.esa.es", 19), ("dial055.mbnet.mb.ca", 19), ("tigger.nashscene.com", 19))
        val res= LogAnalyseFuns.top25ErrorCodeResponseHosts(access_logs)
        assert(top25ErrorResponseCodeHosts===res)
  }

  test("Response Codes Per Day"){

    val errorCodesPerDay= List((1, 243), (3, 303), (4, 346), (5, 234), (6, 372), (7, 532), (8, 381), (9, 279), (10, 314), (11, 263), (12, 195), (13, 216), (14, 287), 
        (15, 326), (16, 258), (17, 269), (18, 255), (19, 207), (20, 312), (21, 305), (22, 288)) 
    val res= LogAnalyseFuns.responseErrorCodesPerDay(access_logs)
        assert(errorCodesPerDay===res)  
  }
  
  
    test("Response Codes Per Hour"){

    val errorCodesPerHour= List((0, 175), (1, 171), (2, 422), (3, 272), (4, 102), (5, 95), (6, 93), (7, 122), (8, 199), (9, 185), (10, 329), (11, 263), (12, 438), (13, 397), 
          (14, 318), (15, 347), (16, 373), (17, 330), (18, 268), (19, 269), (20, 270), (21, 241), (22, 234), (23, 272))
    val res= LogAnalyseFuns.errorResponseCodeByHour(access_logs)
    assert(errorCodesPerHour===res)  
  }
  
   test("Average Number Of Requests Per Week Day"){

    val avgReqNrPerWeekDay= List()
    val res=LogAnalyseFuns.getAvgRequestsPerWeekDay(access_logs)
    assert(res===List((34016,"Monday"), (33953,"Tuesday"), (57589,"Wednesday"), (52685,"Thursday"), (58554,"Friday"), (53870,"Saturday"), (59013,"Sunday")))

   }
    
  override protected def afterAll() {

     if (sc!=null) {sc.stop; println("Spark stopped......")}
     else println("Cannot stop spark - reference lost!!!!")
  }
}