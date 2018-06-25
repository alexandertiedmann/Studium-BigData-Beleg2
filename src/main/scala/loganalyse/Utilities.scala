package loganalyse

import org.apache.spark.SparkContext
import java.time.format.DateTimeFormatter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.OffsetDateTime
import java.io.File
import java.io.PrintWriter
import java.nio.charset.Charset 
import scala.io.Codec.charset2codec

object Utilities {
  
  val hadoopfs="hdfs://localhost:9000/user/hendrik/"
  val FILENAME_ACCESS_PATTERN ="""^(.+)\/(\w+\.\w+)$""".r
  val APACHE_ACCESS_LOG_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*) *" (\d{3}) (\S+)""".r
   
  val DATE_TIME_FORMAT="dd/MMM/yyyy:HH:mm:ss Z"
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT);

  
  def getData(filename:String, source:String, sc:SparkContext):RDD[String]={
    
    if (source.equals("resources")) {
      
      val url=getClass.getResource("/"+filename).getPath
      val path = url match{
    
        case FILENAME_ACCESS_PATTERN(path, name) => (path, name)
        case _ => ("","")
      }
      sc.textFile("file://"+url)
    }
    
    else if (source.equals("hadoop-fs")){
      
      sc.textFile(hadoopfs+filename)    
    }
    else null
  }
      
  def parse_line(line:String):(Row,Int)={

  /* Row composition
   * 
   * 0 - String: IP or name  
   * 1 - String: Client ID on user machine
   * 2 - String: User name
   * 3 - OffsetDateTime: Date and time of request
   * 4 - String: Request Method
   * 5 - String: Request endpoint
   * 6 - String: Protocol
   * 7 - Integer: Response Code
   * 8 - Integer: Content size
   * 
   */
    
    line match {
  
      case APACHE_ACCESS_LOG_PATTERN(host, client_identd, user_id, date_time, method,
          endpoint, protocol, response_code, content_size) => 
      (Row(host, client_identd, user_id, OffsetDateTime.parse(date_time, formatter), method,
          endpoint, protocol, response_code.toInt, 
          {if (content_size=="-") 0 else try {content_size.toInt} catch {case _:NumberFormatException => 0}}) ,1)

      case _ => (Row(line),0)
    } 
 }
  
 def AccessLogRowToText(row:Row):String={
  
    return row.getString(0)+" "+row.getString(1)+" "+row.getString(2)+
       " ["+formatter.format(row.get(3).asInstanceOf[OffsetDateTime])+"] \"" +
       row.getString(4)+" "+row.getString(5)+" "+row.getString(6)+"\" "+
       row.getInt(7).toString+" "+row.getInt(8).toString
  }
 
 def filterFile(filenameIn:String, filenameOut:String, filterFun:(Row)=>Boolean):Unit={
 
   val url=getClass.getResource("/"+filenameIn).getPath
   val path = url match{
    
     case FILENAME_ACCESS_PATTERN(path, name) => (path, name)
     case _ => ("","")
   }

   val stream= this.getClass.getResourceAsStream("/"+filenameIn)
   val lines= scala.io.Source.fromInputStream(stream)(Charset.forName("ISO-8859-15")).getLines
   
   val fileOut= new File(path._1+"/"+filenameOut)
   val pw:PrintWriter= new PrintWriter(fileOut,"ISO-8859-15")
   
   var row_error= 0
   var row_ok=0
   var filtered=0
   
   for (l <- lines) {
     
     val row= parse_line(l)
     if (row._2==0) {println(l);row_error=row_error+1}
     else{
       
       row_ok=row_ok+1
       if (filterFun(row._1)){
         pw.println(l)
         //pw.println(AccessLogRowToText(row._1))  
       }
       else filtered=filtered+1
     }
   }
   
   pw.close
   println("Recognized rows: "+row_ok)
   println("Erroneous rows: "+row_error)
   println("Filtered rows: "+filtered)

 }
 
 def getDateFromTo(fileIn:String, fileOut:String, from:Int, to:Int):Unit={

   val start=OffsetDateTime.parse(from.toString+"/Jul/1995:00:00:00 -0400", formatter)
   val end=OffsetDateTime.parse(to.toString+"/Jul/1995:00:00:00 -0400", formatter)
   
   Utilities.filterFile(fileIn, fileOut, row=>{
    
     val date= row.get(3).asInstanceOf[OffsetDateTime]
     if ((start.compareTo(date)<=0) && (date.compareTo(end)<=0)) true
     else false
  })  
 
 }
}