package sink

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import source.SensorReading

object Main_ES {
	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		val sensorReadings: DataStream[String] = env.readTextFile("target/classes/sensors.txt")
		// 先转换为样例类
		val dataStream: DataStream[SensorReading] = sensorReadings.map(data => {
			val arr: Array[String] = data.split(",")
			SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
		})

		// 定义httpHosts
		val httpHosts: util.List[HttpHost] = util.Arrays.asList(new HttpHost("localhost", 9200))
		val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
			override def process(t: SensorReading,
								 runtimeContext: RuntimeContext,
								 requestIndexer: RequestIndexer): Unit = {
				// 包装一个Map作为dataSource
				val dataSource = new util.HashMap[String, String]()
				dataSource.put("id", t.id)
				dataSource.put("temperature", t.temperature.toString)
				dataSource.put("timestamp", t.timestamp.toString)

				// 创建RequestIndexer
				val indexRequest = Requests.indexRequest()
    				.index("sensor")
    				.`type`("readingdata")
    				.source(dataSource)

				requestIndexer.add(indexRequest)

			}
		}

		val esSink: ElasticsearchSink[SensorReading] = new ElasticsearchSink
			.Builder[SensorReading](httpHosts, myEsSinkFunc)
			.build()
		dataStream.addSink(esSink)

		env.execute()
	}
}