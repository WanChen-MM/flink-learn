package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

object Main_WindowFunction {
	// 窗口函数分为两类
	// 	增量聚合函数 每条数据到来就进行计算，保持一个简单的状态
	// 		ReduceFunction、AggregateFunction
	// 	全窗口函数 先把窗口所有数据收集起来，等到计算的时候会遍历所有数据
	// 		ProcessWindowFunction
	// 其他可选API
	// 	trigger 触发器
	// 		定义window什么时候关闭，触发计算结果
	// 	evictor 移除器
	// 		定义移除某些数据的逻辑
	// 	allowedLateness 允许处理迟到的数据
	// 	sideOutputLateData 将迟到的数据放入侧输出流
	// 	getSideOutput 获取侧输出流出

	// stream
	// 	.keyBy
	// 	.window
	// 	[.trigger]
	// 	[.allowedLateness]
	// 	[.sideOutputLateData]
	// 	.reduce/aggregate/fold/apply
	// 	[.getSideOutput]





	def main(args: Array[String]): Unit = {
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
			.timeWindow(Time.seconds(15)) // 滚动时间窗口
			// .reduce()定义计数

		env.execute()
	}
}
