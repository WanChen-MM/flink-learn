package wordcount.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object Main {
	def main(args: Array[String]): Unit = {
		// 1. 创建一个批处理执行环境
		val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

		// 2. 读取数据
		val lines: DataSet[String] = env.readTextFile("target/classes/wordcount.txt")

		// 对数据进行转换处理统计、分词、分组、聚合
		val resultDataset: AggregateDataSet[(String, Int)] = lines.flatMap(_.split(" "))
			.map((_, 1))
			.groupBy(0)
			.sum(1)

		resultDataset.print()
	}
}
