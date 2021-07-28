package transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import source.SensorReading

object Main {
	// 1. map
	// 2. flatMap
	// 3. filter

	// 4. keyBy DataStream -> KeyedStream
	// 滚动聚合算子：
	// 	sum
	// 	min 取出最小值字段，其他字段值不会变
	// 	max
	// 	minBy 取出最小值的记录
	// 	maxBy
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
		val aggStream: DataStream[SensorReading] = dataStream
			.keyBy("id")
			.min("temperature")

		// 需要输出最小的温度值和最近的时间戳
		val reduceStream: DataStream[SensorReading] = dataStream
			.keyBy("id")
			.reduce((data, data2) => {
				SensorReading(data.id, data2.timestamp, data.temperature.min(data2.temperature))
			})
		reduceStream.print()
		env.execute()
	}
}
