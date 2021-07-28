package source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object Main_FileSource {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		sensorReadings.print()
		env.execute()
	}
}
