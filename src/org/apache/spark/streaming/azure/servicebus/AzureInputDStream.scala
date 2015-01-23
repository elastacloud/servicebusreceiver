package org.apache.spark.streaming.azure.servicebus

import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by Richard on 12/5/2014.
 */


private [streaming]
class AzureInputDStream(
                         @transient ssc_ : StreamingContext,
                         receiver : AzureServiceBusSession,
                         filters: Seq[String],
                         storageLevel: StorageLevel
                         ) extends ReceiverInputDStream[String](ssc_) with Logging {


  def getReceiver(): Receiver[String] = {
    new AzureServiceBusReceiver(receiver, filters, storageLevel).asInstanceOf[Receiver[String]]
  }
}


