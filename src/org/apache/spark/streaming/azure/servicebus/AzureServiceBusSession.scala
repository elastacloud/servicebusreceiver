package org.apache.spark.streaming.azure.servicebus

import org.apache.spark.Logging
import scalaj.http.Http

/**
 * Created by Richard on 12/6/2014.
 */
class AzureServiceBusSession(namespace: String, topicName : String, subscriptionName : Option[String], sas : String)
  extends Serializable with Logging
{


  def this(namespace: String, queueName: String, sas: String) {
    this(namespace, queueName, None, sas)
  }

  val queueRequest = "https://%s.servicebus.windows.net/%s/messages/head?timeout=5".format(namespace, topicName)
  // http{s}://{serviceNamespace}.servicebus.windows.net/{topicPath}/subscriptions/{subscriptionName}/messages/head
  lazy val topicRequest = "https://%s.servicebus.windows.net/%s/subscriptions/%s/messages/head?timeout=5".format(namespace, topicName, subscriptionName.get)

  val hostHeader = "%s.servicebus.windows.net" format namespace
  val authorisationHeader = "SharedAccessSignature %s" format sas

  def requestTypeFunc = subscriptionName match {
    case Some(_) => topicRequest
    case _ => queueRequest
  }

  val requestType = requestTypeFunc

  def receiveNext = {
    val result = Http(requestType)
      .method("DELETE")
      .header("Authorization", authorisationHeader)
      .header("Host", hostHeader)
      .header("User-Agent", "Brisk")
      .asString
    if(result.code != 200 && result.code != 201) {
      logInfo("Message returned with result code : " + result.code)
      logInfo("Message returned with result header : " + authorisationHeader)
      logInfo("URI : " + requestType)
    }
    result.body
  }

  private def evaluateResponseCode(responseCode: Int): Boolean = {
    responseCode match {
      case 200 => true
      case _ => false
    }
  }
}

