
import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import scala.reflect.ClassTag


/**
  * @author y00267925
  */
trait SparkProcess extends Serializable {
  protected def process(ssc: StreamingContext, sc: SparkContext, sqlContext: SQLContext)

  protected def startStreaming(checkpointPath: String, periodsSeconds: Int) = {

    val conf = new SparkConf()

    //优雅的关闭
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    //开启反压机制
    conf.set("spark.streaming.backpressure.enabled", "true")

    val creatingFunc = () => {
      val ssc = new StreamingContext(conf, Durations.seconds(periodsSeconds));
      val sc: SparkContext = ssc.sparkContext
      val sqlContext = new SQLContext(sc)
      ssc.checkpoint(checkpointPath)
      process(ssc, sc ,sqlContext)
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunc)
    val logger: Logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.WARN)
    ssc.start();
    ssc.awaitTermination();
  }

  protected def updateFunc[K: ClassTag](stream: DStream[(K, Seq[String])], getTime: (Seq[String]) => String, fun1: (Seq[Seq[String]], Seq[String]) => Option[Seq[Seq[String]]]): DStream[(K, Seq[Seq[String]])] = {
    stream.updateStateByKey((v, s: Option[Seq[Seq[String]]]) => {
      val his = if (s.isDefined) {
        val t = s.get.filter {
          getTime(_) > DateFormatUtils.format(new Date, "yyyyMMdd")
        }
        if (t.isEmpty) None else t
      } else None
      val last = if (v.size > 0) {
        v.sortBy {
          getTime(_)
        }.last
      } else None

      (his, last) match {
        case (his: Seq[Seq[String]], None) => Some(his)
        case (None, last: Seq[String]) => Some(Seq(last))
        case (his: Seq[Seq[String]], last: Seq[String]) => fun1(his, last)
        case _ => None
      }
    })
  }
}
