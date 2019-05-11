import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object CountWebsiteHits {

def main (args :Array[String]) :Unit = {
     val conf =new SparkConf().setAppName("countwebsitehits")
     val sc =new SparkContext(conf);
	 val ssc = new StreamingContext(sc, Seconds(30))	
	 val kafkaStream = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer-group",Map("goshopping_webclicks" -> 1))  
	 ssc.checkpoint("/home/vagrant/bigdata/SparkCheckpoint")
	 val lines = kafkaStream.map{case(a,b)=>b}
	 val words = lines.map(x=>(x.split("\t")(5),1))
     val reduced = words.reduceByKeyAndWindow((a,b)=>a+b,Seconds(60))
	 reduced.saveAsTextFiles("/GoShopping/hitscount")
     ssc.start()
     ssc.awaitTermination()
	 }
	}