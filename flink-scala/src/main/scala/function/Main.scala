package function

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import source.SensorReading

class Main {
	// 1. 富函数， DataStream API提供的一个函数类的接口，所有Flink函数类都有其Rich版本。
	// 	它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
}

class MyRichFunction extends RichMapFunction[SensorReading, String] {
	// 做一些初始化的操作，比如数据库连接
	override def open(parameters: Configuration): Unit = super.open(parameters)

	override def map(in: SensorReading): String = in.id + " temperature"

	// 做一些收尾工作。比如关闭数据库连接
	override def close(): Unit = super.close()
}