package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

object Main_KafkaSource {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val properties = new Properties()
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "169.254.50.101:9092")
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group")
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
		val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
		stream.print()
		env.execute()
	}
}
