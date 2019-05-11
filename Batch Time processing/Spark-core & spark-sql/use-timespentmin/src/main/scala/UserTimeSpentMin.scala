case class WebClicksData(accessdate:String,accesstime:String,hostip:String,csmethod:String,customer_ip:String,domain:String,product:String,product_type:String,timespent:Int,redirected:String,devicetype:String)
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object UserTimeSpentMin {
def main ( args : Array[String]) :Unit ={
val conf =new SparkConf().setAppName("userTimeSpentmin");
val sc =new SparkContext(conf);
val sqlContext = new SQLContext(sc);

import sqlContext.implicits._;
val web=sc.textFile(args(0),2);
val cleaned_web =web.map(x => x.trim).filter(x=>x.length != 0)
val webclick_Multi =cleaned_web.map(x=>x.split("\t"))
val webclick_Multi_col = webclick_Multi.map(x=>(x(0),x(1),x(2),x(3),x(4),x(5).split("\\.")(1),x(5).split("\\&")(0).split("=")(1),x(5).split("=")(2),x(6).toInt,x(7),x(8)))

val WEBcased=webclick_Multi_col.map{case(a,b,c,d,e,f,g,h,i,j,k)=>WebClicksData(a,b,c,d,e,f,g,h,i,j,k)}
val webdf=WEBcased.toDF()
webdf.createOrReplaceTempView("webclicks") 
val webresult = sqlContext.sql("select customer_ip,sum(timespent) from webclicks group by customer_ip order by sum(timespent) ")
webresult.show(1,false)
}
}
