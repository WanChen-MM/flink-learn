package transform

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import source.SensorReading

object Main_Connect {
	// connect coMap
	// connect DataStream -> ConnectedStreams
	// CoMap CoFlatMap
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
		// 推荐使用副输出
		val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
			if (data.temperature > 30.0) {
				Seq("high")
			} else {
				Seq("low")
			}
		})
		val highTempStream: DataStream[SensorReading] = splitStream.select("high")
		val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
		val allTempStream: DataStream[SensorReading] = splitStream.select("low", "high")

		val warningStream: DataStream[(String, Double)] = highTempStream.map(data => {
			(data.id, data.temperature)
		})

		val cnStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)
		val resultStream: DataStream[Product] = cnStream.map(
			warnData => (warnData._1, warnData._2, "warn"),
			lowData => (lowData.id, "healthy"))
		resultStream.print()
		env.execute()
	}

}
