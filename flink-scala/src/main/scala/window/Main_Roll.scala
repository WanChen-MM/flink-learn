package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

object Main_Roll extends App {
	val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
	env.setParallelism(1)
	val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
	// 先转换为样例类
	val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
		val arr: Array[String] = data.split(",")
		SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
	})

	dataStream
		.map(data => (data.id, data.temperature))
		.keyBy(_._1)
		.window(TumblingEventTimeWindows.of(Time.seconds(15))) // 滚动时间窗口

	dataStream
		.map(data => (data.id, data.temperature))
		.keyBy(_._1)
		.window(SlidingEventTimeWindows.of(Time.seconds(15)
			, Time.seconds(10))) // 滑动时间窗口

	dataStream
		.map(data => (data.id, data.temperature))
		.keyBy(_._1)
		.window(EventTimeSessionWindows.withGap(Time.seconds(10))) // 会话窗口

	// 简写方式
	dataStream
		.map(data => (data.id, data.temperature))
		.keyBy(_._1)
		.timeWindow(Time.seconds(15)) // 滚动时间窗口
	dataStream
		.map(data => (data.id, data.temperature))
		.keyBy(_._1)
		.timeWindow(Time.seconds(15), Time.seconds(10))

	env.execute()
}
