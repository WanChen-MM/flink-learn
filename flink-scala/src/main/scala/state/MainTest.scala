package state

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import source.SensorReading

object MainTest {
	// 对于温度传感器，对于温度值跳变，超过10度，报警
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
		val value: DataStream[(String, Double, Double)] = dataStream
			.keyBy(_.id)
			//.flatMap(new MyFlatMapFunction(10.0))
			.flatMapWithState[(String, Double, Double), Double]({
				case (data: SensorReading, None) => (List.empty, Some(data.temperature)) // 初始值
				case (data: SensorReading, lastTemp: Some[Double]) => {
					val diff: Double = (lastTemp.get - data.temperature).abs
					if (diff > 10.0) {
						(List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
					} else {
						(List.empty, Some(data.temperature))
					}
				}
			})

		value.print()

		env.execute()
	}
}

class MyFlatMapFunction(threshold: Double = 10.0) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
	// 定义状态，保存上一次温度值
	lazy val lastTempState: ValueState[Double] =
		getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]))

	// 需要处理第一个值
	override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
		val lastTemp = lastTempState.value();
		val diff: Double = (lastTemp - in.temperature).abs
		if (diff > threshold) {
			collector.collect((in.id, lastTemp, in.temperature))
		}
		lastTempState.update(in.temperature)
	}
}
