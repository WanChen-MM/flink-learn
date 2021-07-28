package transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import source.SensorReading


object Main_Union {
	// union要求两个Stream的数据类型一致
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
		val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)
		unionStream.print()
		env.execute()
	}
}
