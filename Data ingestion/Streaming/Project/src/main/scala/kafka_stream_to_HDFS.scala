import org.apache.spark.streaming._

import org.apache.spark._

import org.apache.spark.streaming._
 
import org.apache.spark.streaming.kafka._


object kafka_stream_to_HDFS{

    

	def main ( args : Array[String]) :Unit ={
	  val conf = new SparkConf().setAppName("kafka_stream_to_HDFS")
      val sc = new SparkContext(conf)
      val ssc= new StreamingContext(sc,Seconds(3))
	  
      ssc.checkpoint("/home/vagrant/bigdata/SparkCheckpoint")
      val kafkaStream = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer-group",Map("t10" -> 1))
	  val lines = kafkaStream.map{case(a,b) => b}
	  lines.foreachRDD(t=> {
         val test = t.map(x=>x)
         test.saveAsTextFile(args(0))
		})
         

	  ssc.start()
	  ssc.awaitTermination()
	  
	  }
	  
	 
	  
}

