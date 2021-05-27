package custom_listener
import org.apache.spark.SparkConf

import scala.concurrent.duration._

object Configs {

  def logDuration(sparkConf: SparkConf): Boolean = {
    sparkConf.getBoolean("spark.delight_aug.logDuration", defaultValue = false)
  }
  def pollingInterval(sparkConf: SparkConf): FiniteDuration = {
    sparkConf.getDouble("spark.delight_aug.pollingIntervalSecs", 0.5).seconds
  }

  def maxPollingInterval(sparkConf: SparkConf): FiniteDuration = {
    sparkConf.getDouble("spark.delight_aug.maxPollingIntervalSecs", 60).seconds
  }

  def maxWaitOnEnd(sparkConf: SparkConf): FiniteDuration = {
    sparkConf.getDouble("spark.delight_aug.maxWaitOnEndSecs", 10).seconds
  }

  def bufferMaxSize(sparkConf: SparkConf): Int = {
    sparkConf.getInt("spark.delight_aug.buffer.maxNumEvents", 1000)
  }

  def waitForPendingPayloadsSleepInterval(sparkConf: SparkConf): FiniteDuration = {
    sparkConf.getDouble("spark.delight_aug.waitForPendingPayloadsSleepIntervalSecs", 1).seconds
  }

  def payloadMaxSize(sparkConf: SparkConf): Int = {
    sparkConf.getInt("spark.delight_aug.payload.maxNumEvents", 10000)
  }


  def writeFilePath(sparkConf: SparkConf): String = {
    sparkConf.get("spark.delight_aug.writePath", "/dbfs/FileStore/")
  }

  def appId(sparkConf: SparkConf): String = {
    sparkConf.get("spark.app.id", "notDefined")
  }

}
