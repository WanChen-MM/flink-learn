package time

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

object Main_Window {
	// 窗口起点：
	// 	timestamp - (timestamp - offset + windowSize) % windowSize
	def main(args: Array[String]): Unit = {
		// 设置时间语义
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(100)
		val params: ParameterTool = ParameterTool.fromArgs(args)
		val hostname: String = params.get("hostname", "localhost")
		val port: Int = params.getInt("port", 7777)

		val lateTag = new OutputTag[SensorReading]("late")

		val socketDataStream: DataStream[String] = env.socketTextStream(hostname, port)

		val dataStream: DataStream[SensorReading] = socketDataStream.map(data => {
			val arr = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
			.assignTimestampsAndWatermarks(
				new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
					override def extractTimestamp(element: SensorReading): Long = {
						element.timestamp * 1000
					}
				})
		val resultStream: DataStream[SensorReading] = dataStream.keyBy("id")
			.timeWindow(Time.seconds(8))
			.allowedLateness(Time.minutes(15))
			.sideOutputLateData(lateTag)
			.reduce((curRes, newData) => {
				SensorReading(curRes.id, newData.timestamp, curRes.temperature.min(newData.temperature))
			})
		resultStream.getSideOutput(lateTag).print("SIDE")
		resultStream.print()
		env.execute()
	}
}
