package state

import java.util

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.scala._
import source.SensorReading

object Main {
	// 1. 状态类型
	// 	算子状态 限定访问范围为并行子任务
	// 		数据类型：
	// 			列表状态（List State）
	// 			联合列表状态（Union List State）：它与常规列表状态的区别在于，在发生故障时，或者从保存点（savepoint）启动应用程序时如何恢复。
	// 			广播状态：一个算子有多项任务，而它的每项任务状态又都相同，那么这种状态适合应用广播状态。

	// 	键控状态	限定访问范围为当前key [用的比较多]
	// 		数据结构：
	// 			值状态
	// 			列表状态
	// 			映射状态
	// 			聚合状态

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})


		env.execute()
	}
}

class MyRichMapper extends RichMapFunction[SensorReading, String] {

	lazy val valueState: ValueState[Double] =
		getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))

	lazy val listState: ListState[Int] =
		getRuntimeContext.getListState(new ListStateDescriptor[Int]("liststate", classOf[Int]))

	lazy val mapState: MapState[String, Double] =
		getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))

	lazy val reducingState: ReducingState[SensorReading] =
		getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reducestate", new MyReducer, classOf[SensorReading]))

	/*
		override def open(parameters: Configuration): Unit = {
			valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))
		}
	*/
	override def map(in: SensorReading): String = {
		val value: Double = valueState.value()
		valueState.update(in.temperature.min(value))

		listState.add(1)
		val ints = new util.ArrayList[Int]()
		ints.add(2)
		ints.add(3)
		listState.addAll(ints)
		// 替换掉整个liststate
		listState.update(ints)
		listState.get() // 获取整个list的值

		mapState.contains("sensor_1")
		mapState.get("sensor_1")
		mapState.put("sensor_1", 1.3)

		reducingState.get()
		reducingState.add(SensorReading("sensor_1", 1547718213, 1.3))

		in.id
	}
}

class MyReducer extends ReduceFunction[SensorReading] {
	override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
		SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
	}
}
