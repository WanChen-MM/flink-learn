package source.custom

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import source.SensorReading

import scala.util.Random

object Main {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val value: DataStream[SensorReading] = env.addSource(new MySensorSource)
		value.print()
		env.execute()
	}
}

// 自定义SourceFunction
class MySensorSource extends SourceFunction[SensorReading] {
	@volatile var running: Boolean = true

	override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
		// 定义无限循环，不停地产生数据
		val random = new Random()

		// 随机生成10随机温度
		var curTemp = 1.to(10).map(i => ("sensor_" + i, random.nextDouble() * 100))
		while (running) {
			curTemp = curTemp.map(data => (data._1, data._2 + random.nextGaussian()))
			val curTime: Long = System.currentTimeMillis()
			curTemp.foreach(data => {
				ctx.collect(SensorReading(data._1, curTime, data._2))
			})
			TimeUnit.MILLISECONDS.sleep(10000)
		}
	}

	override def cancel(): Unit = {
		running = false
	}
}