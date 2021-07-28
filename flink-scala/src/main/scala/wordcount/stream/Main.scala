package wordcount.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Main {
	def main(args: Array[String]): Unit = {
		// 1. 创建流处理执行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

		// 针对执行环境设置并行度，还可以单独针对算子设计并行度
		env.setParallelism(4)

		val params: ParameterTool = ParameterTool.fromArgs(args)
		val hostname: String = params.get("hostname", "localhost")
		val port: Int = params.getInt("port", 7777)


		// 2. 接收socket文本流
		val socketDataStream: DataStream[String] = env.socketTextStream(hostname, port)

		// 3. 统计
		val resultDs: DataStream[(String, Int)] = socketDataStream.flatMap(_.split(" "))
			.map((_, 1))
			.keyBy(0)
			.sum(1)
		resultDs.print()
		env.execute()
	}
}
