package custom_listener
import org.apache.spark.{JsonProtocolProxy, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListenerEvent
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, render}
import java.util.concurrent.atomic.AtomicBoolean
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.FiniteDuration

class metrics_logger_aug(sparkConf: SparkConf) extends Logging  {

  private val pendingEvents: mutable.Queue[SparkListenerEvent] = new mutable.Queue[SparkListenerEvent]()
  private val eventsBuffer: mutable.Buffer[SparkListenerEvent] = mutable.Buffer()

  private val shouldLogDuration = Configs.logDuration(sparkConf)
  private val pollingInterval = Configs.pollingInterval(sparkConf)
  private val bufferMaxSize = Configs.bufferMaxSize(sparkConf)
  private val maxWaitOnEnd = Configs.maxWaitOnEnd(sparkConf)
  private val waitForPendingPayloadsSleepInterval = Configs.waitForPendingPayloadsSleepInterval(sparkConf)
  private val maxPollingInterval = Configs.maxPollingInterval(sparkConf)
  private val payloadMaxSize = Configs.payloadMaxSize(sparkConf)
  private val writeFilePath = Configs.writeFilePath(sparkConf)
  private var currentPollingInterval = pollingInterval

  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
  private val started: AtomicBoolean = new AtomicBoolean(false)

  def enqueueEvent(event: SparkListenerEvent, flush: Boolean = false, blocking: Boolean = false): Unit = time(
    shouldLogDuration, "enqueueEvent"
  ) {
      val bufferSize = eventsBuffer.synchronized {
        eventsBuffer += event
        eventsBuffer.length
      }
      startIfNecessary()
      if (flush || bufferSize >= bufferMaxSize) flushEvents(blocking)

  }

  private def flushEvents(blocking: Boolean = false): Unit = time(shouldLogDuration, "flushEvents") {

    eventsBuffer.synchronized {
      pendingEvents.synchronized {
        if (eventsBuffer.nonEmpty) {
          val bufferContent = eventsBuffer.to[immutable.Seq]
          pendingEvents.enqueue(bufferContent: _*)
          logInfo(s"Flushing  events, now pending events")
          eventsBuffer.clear()
        } else {
          logWarning("Nothing to flush")
        }
      }
    }
    if(blocking) waitForPendingEvents()
  }

  private def waitForPendingEvents(): Unit = time(shouldLogDuration, "waitForPendingEvents") {
    val startWaitingTime = currentTime
    var nbPendingEvents: Int = pendingEvents.synchronized(pendingEvents.size)
    while((currentTime - startWaitingTime) < maxWaitOnEnd.toMillis && nbPendingEvents > 0) {
      Thread.sleep(waitForPendingPayloadsSleepInterval.toMillis)
      logInfo(s"Waiting for all pending events to be sent")
      nbPendingEvents = pendingEvents.synchronized(pendingEvents.size)
    }
    if(nbPendingEvents > 0) {
      logWarning(s"Stopped waiting for pending events to be sent")
    }
  }

  private def startRepeatThread(interval: FiniteDuration)(action: => Unit): Thread = {
    val thread = new Thread {
      override def run() {
        while (true) {
          val start = currentTime
          val _ = action
          val end = currentTime
          Thread.sleep(math.max(interval.toMillis - (end - start), 0))
        }
      }
    }
    thread.start()
    thread
  }

  private def startIfNecessary(): Unit = time(shouldLogDuration, "startIfNecessary") {
    if(started.compareAndSet(false, true)) {
      startRepeatThread(currentPollingInterval) {
        publishPendingEvents()
      }
      logInfo("Started writer polling thread")
    }
  }

  private def publishPendingEvents(): Unit = time(shouldLogDuration, "publishPendingEvents") {
    var errorHappened = false
    var nbPendingEvents: Int = pendingEvents.synchronized(pendingEvents.size)
    while(nbPendingEvents > 0 && !errorHappened) {
      try {
        val firstEvents = pendingEvents.synchronized(pendingEvents.take(payloadMaxSize)).to[immutable.Seq]
        val serializedEvents = firstEvents.flatMap(serializeEvent)
        val data = serializedEvents.mkString("","\n","\n")

        if (Files.exists(Paths.get(writeFilePath)))  {
          Files.write(Paths.get(writeFilePath), data.getBytes(StandardCharsets.UTF_8),StandardOpenOption.APPEND)
        }
        else {
          Files.write(Paths.get(writeFilePath), data.getBytes(StandardCharsets.UTF_8))
        }

        pendingEvents.synchronized( // if everything went well, actually remove the payload from the queue
          for(_ <- 1 to firstEvents.length) pendingEvents.dequeue()
        )
        if(currentPollingInterval > pollingInterval) {
          // if everything went well, polling interval is set back to its initial value
          currentPollingInterval = pollingInterval
          logInfo(s"Polling interval back to ${pollingInterval.toSeconds}s because last payload was successfully sent")
        }
      } catch {
        case e: Exception =>
          errorHappened = true // stop sending payload queue when an error happened until next retry
          currentPollingInterval = (2 * currentPollingInterval).min(maxPollingInterval)  // exponential retry
          logWarning(s"Polling interval increased to ${currentPollingInterval.toSeconds}s because last payload failed", e)
      }
      nbPendingEvents = pendingEvents.synchronized(pendingEvents.size)
    }
  }

  private def serializeEvent(event: SparkListenerEvent): Option[String] = {
    try {
      Some(compact(render(JsonProtocolProxy.jsonProtocol.sparkEventToJson(event))))
    } catch {
      case e: Exception =>
        logWarning(s"Could not serialize event of type ${event.getClass.getCanonicalName} because: ${e.getClass.getCanonicalName}")
        None
    }
  }
}

object metrics_logger_aug {

  private var sharedConnector: Option[metrics_logger_aug] = None

  def getOrCreate(sparkConf: SparkConf): metrics_logger_aug = {
    if(sharedConnector.isEmpty) {
      sharedConnector = Option(new metrics_logger_aug(sparkConf))
    }
    sharedConnector.get
  }
}