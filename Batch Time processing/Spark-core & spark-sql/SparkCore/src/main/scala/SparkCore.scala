import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkCore {
def main ( args : Array[String]) :Unit ={
val conf =new SparkConf().setAppName("sparkCore");
val sc =new SparkContext(conf);
val web=sc.textFile(args(0),2);
val cleaned_web =web.map(x => x.trim).filter(x=>x.length != 0)
val webclick_Multi =cleaned_web.map(x=>(x.split("\t")(4),x.split("\t")(6).toInt))
val webclick_Multi_col = webclick_Multi.reduceByKey((a,b)=>a+b)
print("Maximum Time Spent : " + webclick_Multi_col.reduce((x,y) => if(x._2>y._2 ) x else y ))
}
}