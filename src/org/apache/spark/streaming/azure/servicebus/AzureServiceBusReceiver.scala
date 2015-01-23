package org.apache.spark.streaming.azure.servicebus

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by Richard on 12/6/2014.
 */
//private[streaming]
class AzureServiceBusReceiver(
                               receiver: AzureServiceBusSession,
                               filters: Seq[String],
                               storageLevel: StorageLevel
                               ) extends Receiver[String](storageLevel) with Logging {

  def onStart() {
    logInfo("Service Bus messaging started")
    receive()
  }

  def onStop() {
    logInfo("Service Bus messaging stopped")
  }

  private def receive() {

    while (true) {

      try {
        val message = receiver.receiveNext
        if (message != "") {
          store(message)
        }
      } catch {
        case ex : Exception => logError(ex.getMessage)
      }
    }

  }
}
