package process.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.SensorReading

// 侧输出流
object Main2 {
	def main(args: Array[String]): Unit = {
		// 设置时间语义
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.getConfig.setAutoWatermarkInterval(100)
		val params: ParameterTool = ParameterTool.fromArgs(args)
		val hostname: String = params.get("hostname", "localhost")
		val port: Int = params.getInt("port", 7777)

		val lowTag = new OutputTag[SensorReading]("low")

		val socketDataStream: DataStream[String] = env.socketTextStream(hostname, port)

		val dataStream: DataStream[SensorReading] = socketDataStream.map(data => {
			val arr = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
		val highTempStream = dataStream.process(new SplitTempProcessor(30.0))
		highTempStream.print("high")
		highTempStream.getSideOutput(lowTag).print("low")

		env.execute();
	}
}

class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
	override def processElement(value: SensorReading,
								ctx: ProcessFunction[SensorReading, SensorReading]#Context,
								out: Collector[SensorReading]): Unit = {
		if(value.temperature > threshold) {
			out.collect(value)
		} else {
			ctx.output(new OutputTag[SensorReading]("low"), value)
		}
	}
}