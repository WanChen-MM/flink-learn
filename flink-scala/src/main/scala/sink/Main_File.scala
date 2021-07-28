package sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import source.SensorReading

object Main_File {
	// stream.addSink(SinkFunction)
	// Flink 提供了很多外部系统的连接器
	// 	kafka、cassandra、ElasticSearch、Hadoop、RabbitMQ。。。

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})

		// dataStream.print()
		// dataStream.writeAsCsv()
		// StreamingFileSink的构造器是protected修饰的，可以使用forRowFormat\forBulkFormat返回对应的构建器
		dataStream.addSink(
			StreamingFileSink.forRowFormat(
				new Path("C:\\Users\\CW\\Desktop\\笔记\\计算机软件\\flink\\flink-scala\\src\\main\\resources"),
				new SimpleStringEncoder[SensorReading]()).build())
		env.execute()
	}
}
