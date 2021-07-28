package fault.tolerant

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Main {
	// 一致性检查点（checkpoints）
	// 		Flink 故障恢复机制的核心。有状态流式应用的一致性检查点，其实就是所有任务的状态，
	// 		在某个时刻点的一份拷贝，这个时间点，应该是所有任务都恰好处理完一个相同的输入数据的时候。

	// 从检查点恢复
	// 		1. 重启应用
	// 		2. 从checkpoint中读取状态，将状态重置
	// 		3. 开始消费

	// 检查点的实现算法
	// 	一种简单的想法：暂停应用，保存状态到检查点，重新恢复应用。Flink没有使用
	// 	Flink检查点算法：基于Chandy-Lamport算法的分布式快照。将检查点的保存和数据处理分开，不暂停整个应用。

	// 检查点分界线
	// 	Flink的检查点算法用到了一种称为分界线的特殊数据形式，用来把一条流上数据按照不同的检查点分开。
	// 	分界线之前到来的数据会导致状态更改，都会被包含在当前分界线所属的检查点中；
	// 	而基于分界线之后的数据导致的所有更改，就会被包含在之后的检查点中。
	//
	// 	分界点对齐
	// 		JobManager会向每个Source任务发送一条带有新检查点ID的信息，通过这种方式来启动检查点。
	// 		数据源将它们的状态写入检查点，并发出一个检查点barrier。
	// 		状态后端在状态存入检查点后，会返回通知source任务，source任务就会想jobmannager确认检查点完成。
	//
	//		下游任务需要等到所有上游任务的分界点到来后才能做检查点（分界点对齐）

	// 开启检查点

	// 重启策略
	// NoRestartStrategyConfiguration
	// FixedDelayRestartStrategyConfiguration
	// FallbackRestartStrategyConfiguration
	// FailureRateRestartStrategyConfiguration

	// 保存点
	// 	原则上创建保存点的算法和检查点完全相同。Flink不会自动创建检查点，因此用户必须明确地触发创建操作。
	//	适用于手动备份、更新应用程序、版本迁移、暂停和重启应用。

	// 要使用savepoint， 建议在算子中指定uid




	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE)
		env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
		env.getCheckpointConfig.setCheckpointTimeout(60000L)
		// 最大允许的并行checkpoint任务，可以防止checkpoint过多。
		env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
		// 两次checkpoint的间隔时间，给真正的任务预留一些时间。与setMaxConcurrentCheckpoints重复
		env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
		env.getCheckpointConfig.setPreferCheckpointForRecovery(true) // 默认从checkpoint恢复
		env.getCheckpointConfig.setTolerableCheckpointFailureNumber(150) // 容忍几次checkpoint失败

		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000L))
		env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(1, TimeUnit.MINUTES)))

	}
}
