package sink.constom

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import source.SensorReading

object Main {
	// 自定义sink- MySQL
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})
		dataStream.addSink(new MyJdbcSinkFunc)
		env.execute()
	}
}

// 缺少一致性保障、故障恢复等
class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {
	// 定义连接、预编译语句
	var conn: Connection = _
	var insertStmt: PreparedStatement = _
	var updateStmt: PreparedStatement = _

	override def open(parameters: Configuration): Unit = {
		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
		insertStmt = conn.prepareStatement("insert into sensor_tmp (id, temp) values (?, ?)")
		updateStmt = conn.prepareStatement("update sensor_tmp set temp = ? where id = ?")
	}

	override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
		// 先执行更新
		updateStmt.setDouble(1, value.temperature)
		updateStmt.setString(2, value.id)
		updateStmt.execute()
		if(updateStmt.getUpdateCount == 0) {
			insertStmt.setString(1, value.id)
			insertStmt.setDouble(2, value.temperature)
		}
	}

	override def close(): Unit = {
		insertStmt.close()
		updateStmt.close()
		conn.close()
	}
}
