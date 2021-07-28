package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

object Main_Test {
	// Flink没有数据不会创建窗口，第一个窗口确定了，后面的窗口就确定了
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.socketTextStream("localhost", 7777)
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})

		// 每15s统计温度最小值
		val winStreamT: DataStream[(String, Double)] = dataStream
			.map(data => (data.id, data.temperature))
			.keyBy(_._1)
			.timeWindow(Time.seconds(15))
			.minBy(1)

		val winStreamR = dataStream
			.keyBy("id")
			.timeWindow(Time.seconds(15))
			.reduce((curData, newData) => {
				SensorReading(curData.id,newData.timestamp, curData.temperature.min(newData.temperature) )
			})
		winStreamT.print("T")
		winStreamR.print("R")

		env.execute()
	}
}
