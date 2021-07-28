package sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import source.SensorReading

object Main_Redis {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
		// 定义FlinkJedisConfigBase
		val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost("localhost")
			.setPort(6379)
			.build()

		dataStream.addSink(new RedisSink[SensorReading](config, new MyRedisMapper))

		env.execute()
	}
}

// 定义RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading] {
	// 定义保存数据写入redis的命令。 Hset 表名 key value
	override def getCommandDescription: RedisCommandDescription =
		new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")

	override def getKeyFromData(t: SensorReading): String = t.id

	override def getValueFromData(t: SensorReading): String = t.temperature.toString
}
