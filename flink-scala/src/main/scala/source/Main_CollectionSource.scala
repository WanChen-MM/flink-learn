package source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object Main_CollectionSource {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		// 1. 从集合中读取数据
		val sensorReadings = List(
			SensorReading("sensor_1", 1547718199, 35.8),
			SensorReading("sensor_6", 1547718201, 15.4),
			SensorReading("sensor_7", 1547718202, 6.7),
			SensorReading("sensor_10", 1547718205, 38.1)
		)
		val sensorRecords: DataStream[SensorReading] = env.fromCollection(sensorReadings)
		sensorRecords.print()
		env.execute()
	}
}

// 定义样例类-温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)
