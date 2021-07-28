package process.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.SensorReading

object Main2 {
	def main(args: Array[String]): Unit = {
		// 设置时间语义
		val env = StreamExecutionEnvironment.getExecutionEnvironment
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
		dataStream.keyBy(_.id)
    			.process(new TempIncreWarning(10*1000))

		env.execute();
	}
}

class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
	// 状态定义：保存上一个温度值进行比较，保存注册定时器的时间戳用于删除
	lazy val lastTempState: ValueState[Double] =
		getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]))

	lazy val lastTimestampState: ValueState[Long] =
		getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTs", classOf[Long]))

	override def processElement(value: SensorReading,
								ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
								out: Collector[String]): Unit = {
		val lastTemp = lastTempState.value()
		val timerTs = lastTimestampState.value()

		lastTempState.update(value.temperature)

		if(value.temperature > lastTemp && timerTs == 0) {
			val ts = ctx.timerService().currentProcessingTime() + interval
			ctx.timerService().registerProcessingTimeTimer(ts)
			lastTimestampState.update(ts)
		}
		else if(value.temperature < lastTemp) {
			ctx.timerService().deleteProcessingTimeTimer(timerTs)
			lastTimestampState.clear()
		}

	}

	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
		out.collect(ctx.getCurrentKey + "连续升温")
		lastTimestampState.clear()
	}
}