package org.apache.spark.streaming.messaging.servicebus

import org.apache.spark.streaming.messaging.AzureMessagingSession

import scalaj.http.Http

/**
 * Created by Richard on 12/6/2014.
 */
class AzureServiceBusSession(namespace: String, entityName : String, subscriptionName : Option[String], sas : String) extends AzureMessagingSession
{

  def this(namespace: String, queueName: String, sas: String) {
    this(namespace, queueName, None, sas)
  }

  def queueRequest (namespace : String, topicName : String) = "https://%s.servicebus.windows.net/%s/messages/".format(namespace, topicName)
  // http{s}://{serviceNamespace}.servicebus.windows.net/{topicPath}/subscriptions/{subscriptionName}/messages/head
  def topicRequest (namespace : String, topicName : String, subscriptionName : String) = {
    "https://%s.servicebus.windows.net/%s/subscriptions/%s/messages/".format(namespace, topicName, subscriptionName)
  }

  def requestTypeFunc = subscriptionName match {
    case Some(_) => topicRequest(namespace, entityName, subscriptionName.get)
    case _ => queueRequest(namespace, entityName)
  }

  val requestType = requestTypeFunc

  def receiveNext = {
    val result = Http("%shead?timeout=5" format requestType)
      .method("DELETE")
      .header("Authorization", authorisationHeader(sas))
      .header("Host", hostHeader(namespace))
      .header("User-Agent", "Brisk")
      .asString
    if(result.code != 200 && result.code != 201) {
      logInfo("Message returned with result code : " + result.code)
      logInfo("Message returned with result header : " + authorisationHeader(sas))
      logInfo("URI : " + requestType)
    }
    result.body
  }

  def send (messageText : String) = {
    val result = Http(requestType)
      .method("POST")
      .header("Authorization", authorisationHeader(sas))
      .header("Host", hostHeader(namespace))
      .header("User-Agent", "Brisk")
      .header("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
      .postData(messageText)
      .asString
    result.code match {
      case 201 => println("message (\"%s\") was sent" format messageText)
      case _ => println("error occurred %d" format result.code)
    }
  }

  private def evaluateResponseCode(responseCode: Int): Boolean = {
    responseCode match {
      case 200 => true
      case _ => false
    }
  }
}

