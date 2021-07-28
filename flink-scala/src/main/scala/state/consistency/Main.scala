package state.consistency

object Main {
	// 状态一致性：计算结果要保证准确
	// 	AT_MOST_ONCE 最多一次，可能数据会丢失。
	// 	AT_LEAST_ONCE 至少一次，可能会有数据重复。
	// 	EXACTLY_ONCE 精确一次性

	// Flink靠一次性检查点保证exactly-once语义。
	// 端到端的精确一致性：
	// 	幂等性
	// 	事务性，构建的事务对应着checkpoint，等到checkpoint真正完成的时候，才会把所有对应的结果写入sink系统中。
	//		实现方式：
	//			预写日志 先写到缓冲，然后一次性输出。Flinkt提供了GenericWriteAheadSink实现

	//			两阶段提交 （2PC）对于每个checkpoint，sink任务会启动一个事务，并将接下来所有接收的数据添加到事务里面。
	//			然后将这些数据写入外部sink系统，但不提交它们——预提交
	//			当它收到checkpoint完成的通知时，它才正式提交事务，实现结果的真正写入。 exactly once
	//			Flink提供了TwoPhaseCommitSinkFunction
	//			对外部sink要求：
	//				1. 能提供事务支持
	//				2. 在checkpoint的间隔期间，必须能够开启一个事务，并接受数据写入。
	//				3. 在收到checkpoint完成的通知之前，事务必须是等待提交的状态。在故障恢复的情况下，这可能需要一些时间。
	//				如果这个时候sink系统关闭事务，那么未提交的数据就会丢失。
	//				4. sink任务必须能够在进程失败后恢复事务。
	//				5. 提交事务必须是幂等性的。

	// 注意异步检查点的时候，jobmanager发出确认信息才可以提交事务。



	def main(args: Array[String]): Unit = {

	}
}
