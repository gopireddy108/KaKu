package com.example.streaming

import java.util.Properties
import javax.mail._
import javax.mail.internet._

object EmailAlert {
  def sendEmailAlert(subject: String, messageText: String, toEmail: String, smtpHost: String, smtpPort: String, smtpUser: String, smtpPassword: String): Unit = {
    val props = new Properties()
    props.put("mail.smtp.host", smtpHost)
    props.put("mail.smtp.port", smtpPort)
    props.put("mail.smtp.auth", "true")
    props.put("mail.smtp.starttls.enable", "true")

    val session = Session.getInstance(props, new Authenticator() {
      override def getPasswordAuthentication: PasswordAuthentication = {
        new PasswordAuthentication(smtpUser, smtpPassword)
      }
    })

    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(smtpUser))
      message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail): _*)
      message.setSubject(subject)
      message.setText(messageText)

      Transport.send(message)
      println("Email sent successfully")
    } catch {
      case e: MessagingException =>
        throw new RuntimeException(e)
    }
  }
}
