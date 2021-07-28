package process.function

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Main3 {
	// 状态后端： 本地状态管理、将检查点状态写入远程存储空间
	// 	MemoryStateBackend
	// 	FsStateBackend
	// 	RocksDBStateBackend
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStateBackend(new FsStateBackend("hdfs://flink/checkpoint"))
		env.setStateBackend(new RocksDBStateBackend("hdfs://flink/checkpoint", true))
	}
}
