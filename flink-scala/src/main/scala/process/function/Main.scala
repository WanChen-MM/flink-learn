package process.function

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.SensorReading

object Main {
	// Flink 提供了8个ProcessFunction
	// ProcessFunction
	// KeyedProcessFunction
	// CoProcessFunction
	// ProcessJoinFunction
	// BroadcastProcessFunction
	// KeyedBroadcastProcessFunction
	// ProcessWindowFunction


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
		}).keyBy(_.id)
			//.process()

		env.execute();
	}
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {


	override def open(parameters: Configuration): Unit = super.open(parameters)


	override def close(): Unit = super.close()

	override def processElement(value: SensorReading,
								ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
								out: Collector[String]): Unit = {
		ctx.getCurrentKey
		ctx.timestamp()
		// ctx.output() 侧输出流
		ctx.timerService().registerEventTimeTimer(1234) // 注册定时器， 定时器在onTimer触发执行
	}

	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)
}