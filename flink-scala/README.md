## Flink是什么

Flink是一个框架和分布式处理引擎，用于对无界和有界数据流进行状态计算。

## 为什么选择Flink

1. 流数据更真实地反映了我们的生活方式
2. 传统的数据架构是基于有限数据集的
3. 低延迟、高吞吐、结果的准确性和良好的容错性

## 哪些行业需要处理流数据

电商和市场营销、物联网、电信业、银行和金融

## 传统数据处理架构

![image-20210625083350000](C:\Users\CW\Desktop\笔记\计算机软件\flink\picture\image-20210625083315461.png)

## 分析处理架构

![image-20210625083920968](C:\Users\CW\Desktop\笔记\计算机软件\flink\picture\image-20210625083920968.png)

有状态的流式框架

![image-20210625084942887](C:\Users\CW\Desktop\笔记\计算机软件\flink\picture\image-20210625084942887.png)

lambda架构

同时存在流式系统和批处理系统，批处理系统保证结果的准确性，流式系统保证低延迟，但是结果不准。

## Flink主要特点

- 事件驱动

  ![img](C:\Users\CW\Desktop\笔记\计算机软件\flink\picture\usecases-eventdrivenapps.png)

- 基于流的世界观

  在Flink的世界观中，一切都是流组成的，离线数据时有界流，实时数据是一个没有界限的流。

  ![img](C:\Users\CW\Desktop\笔记\计算机软件\flink\picture\bounded-unbounded.png)

- 分层API

  ![img](C:\Users\CW\Desktop\笔记\计算机软件\flink\picture\api-stack.png)

- 支持事件时间和处理时间语义

- 精确一次的状态一致性保证

- 低延迟，每秒处理数百万个事件

- 高可用，动态扩展，实现7*24小时全天运行

# Flink部署

```yaml
# flink-conf.yaml
# JobManager runs.
jobmanager.rpc.address: localhost

# The RPC port where the JobManager is reachable.
jobmanager.rpc.port: 6123

# The total process memory size for the JobManager.
#
# Note this accounts for all memory usage within the JobManager process, including JVM metaspace and other overhead.
jobmanager.memory.process.size: 1600m


# The total process memory size for the TaskManager.
#
# Note this accounts for all memory usage within the TaskManager process, including JVM metaspace and other overhead.
taskmanager.memory.process.size: 1728m

# The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.
taskmanager.numberOfTaskSlots: 3

# The parallelism used for programs that did not specify and other parallelism.
parallelism.default: 1


# The failover strategy, i.e., how the job computation recovers from task failures.
# Only restart tasks that may have been affected by the task failure, which typically includes
# downstream tasks and potentially upstream tasks if their produced data is no longer available for consumption.
jobmanager.execution.failover-strategy: region
```

## 提交作业

```scala
1. UI前台提交
2. 后台命令提交
flink run	
```

yarn session-cluster模式

Session-Cluster模式需要先启动集群，然后在提交作业，接着会向yarn申请一块空间，资源永久不变。如果资源满了，下一个作业就无法提交。

Per Job cluster模式

不启动yarn-session，直接执行job

# Flink运行架构

jobmanager 

taskmanager

resourcemanager

dispatcher