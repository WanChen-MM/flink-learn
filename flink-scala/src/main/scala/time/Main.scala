package time

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import source.SensorReading

object Main {
	// watermark 可以设置延迟触发，将事件的时间减慢点，等待延迟事件
	// 	遇到一个时间戳达到了窗口关闭时间，不应该立刻关闭触发窗口计算，而是等待一段时间，等迟到的数据来了再关闭窗口
	// 	watermark 是一种衡量Event Time进展的机制，可以设定延迟触发。
	// 	WaterMark是用于处理乱序事件的，而正确的乱序事件，通常用WaterMark机制结合window来实现。
	// 	数据流中的watermark用于表示timestamp小于watermark的数据都已经到达了，因此窗口的执行也是由watermark触发的。
	// 	watermark用来让程序自己平衡延迟和结果的正确性

	// 关注最大延迟

	// watermark的设定
	// 如果watermark设置的延迟太久，收到结果的速度可能会很慢，解决办法是在水位线到达之前输出一个近似结果
	// 如果watermark到达得太早，则可能收到错误结果，不过Flink处理迟到数据的机制可以解决这个问题

	// 自定义watermark AssignerWithPeriodicWatermarks
	// AssignerWithPunctuatedWatermarks

	def main(args: Array[String]): Unit = {
		// 设置时间语义
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.getConfig.setAutoWatermarkInterval(100)
		val params: ParameterTool = ParameterTool.fromArgs(args)
		val hostname: String = params.get("hostname", "localhost")
		val port: Int = params.getInt("port", 7777)


		// 2. 接收socket文本流
		val socketDataStream: DataStream[String] = env.socketTextStream(hostname, port)
		socketDataStream.map(data => {
			val arr = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
			//.assignAscendingTimestamps(data => data.timestamp * 1000) // 没有乱序，升序时间戳，没必要定义watermark
    		.assignTimestampsAndWatermarks(
				new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
					override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
			}) // 周期性生成器，适用于数据比较密集的场景。
		// 间断性生成器，适用数据比较稀疏的场景。
	}
}
