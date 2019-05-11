import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object CountDevice {
val myUpdateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
  }
def main (args :Array[String]) :Unit = {
     val conf =new SparkConf().setAppName("countDevice")
     val sc =new SparkContext(conf);
	 val ssc = new StreamingContext(sc, Seconds(10))	
	 val kafkaStream = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer-group",Map("goshopping_webclicks" -> 1))  
	 ssc.checkpoint("/home/vagrant/bigdata/SparkCheckpoint")
	 val lines = kafkaStream.map{case(a,b)=>b}
	 val words = lines.map(x=>(x.split("\t")(8),1))
     val reduced = words.updateStateByKey[Int](myUpdateFunc)
	 reduced.saveAsTextFiles("/GoShopping/devicecount")
     ssc.start()
     ssc.awaitTermination()
	 }
	}