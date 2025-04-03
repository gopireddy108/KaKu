package com.example.streaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.streaming.OutputMode
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

object KafkaToKuduStructuredStreaming {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Load configuration
    val config = ConfigFactory.load()

    // Kafka and Kudu configuration
    val kafkaBootstrapServers = config.getString("kafka.bootstrap.servers")
    val kafkaTopic = config.getString("kafka.topic")
    val kuduMaster = config.getString("kudu.master")
    val kuduTable = config.getString("kudu.table")
    val checkpointLocation = config.getString("checkpoint.dir")

    // SMTP configuration for email alerts
    val smtpHost = config.getString("smtp.host")
    val smtpPort = config.getString("smtp.port")
    val smtpUser = config.getString("smtp.user")
    val smtpPassword = config.getString("smtp.password")
    val toEmail = config.getString("smtp.to")

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Kafka to Kudu Streaming")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.backpressure.initialRate", "1000")
      .config("spark.streaming.backpressure.maxRate", "5000")
      .getOrCreate()

    // Set log level for production (INFO or WARN)
    spark.sparkContext.setLogLevel("WARN")

    // Kafka stream and Kudu setup
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .load()

    val messages = kafkaStream.selectExpr("CAST(value AS STRING) as message")

    // Process data and insert into Kudu
    val query = messages.writeStream
      .foreachBatch { (batchDF, batchId) =>
        val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
        insertBatchWithRetry(kuduContext, batchDF, maxRetries = 3, smtpHost, smtpPort, smtpUser, smtpPassword, toEmail)
      }
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkpointLocation)  // Specify checkpoint location
      .start()

    // Ensure that the streaming query handles restarts after failures
    query.awaitTermination()
  }

  def insertBatchWithRetry(kuduContext: KuduContext, batchDF: DataFrame, maxRetries: Int = 3, smtpHost: String, smtpPort: String, smtpUser: String, smtpPassword: String, toEmail: String): Unit = {
    var attempts = 0
    var success = false
    var backoffDuration = 1.seconds // Start with 1 second backoff

    // Retry logic for inserting data into Kudu
    while (attempts < maxRetries && !success) {
      try {
        batchDF.rdd.foreach { row =>
          val kuduRow = new PartialRow()
          kuduRow.addString("message_column", row.getAs[String]("message"))
          kuduContext.insertRows(Seq(kuduRow), "kudu_table_name")
        }
        success = true
        logger.info(s"Batch inserted into Kudu successfully after $attempts attempt(s).")
      } catch {
        case e: KuduException =>
          attempts += 1
          logger.error(s"Error inserting batch into Kudu: ${e.getMessage}. Retrying... (Attempt $attempts of $maxRetries)")
          if (attempts == maxRetries) {
            logger.error(s"Max retries reached for batch insert. Error: ${e.getMessage}")
            val subject = "Critical: Max retries reached for batch insert"
            val messageText = s"Error inserting data into Kudu: ${e.getMessage}. All retry attempts failed."
            EmailAlert.sendEmailAlert(subject, messageText, toEmail, smtpHost, smtpPort, smtpUser, smtpPassword)
            sendToDeadLetterQueue(batchDF) // Send failed batch to Dead Letter Queue
          }
          backoffDuration = backoffDuration * 2
          Thread.sleep(backoffDuration.toMillis)

        case e: Exception =>
          attempts += 1
          logger.error(s"Unexpected error occurred: ${e.getMessage}. Retrying... (Attempt $attempts of $maxRetries)")
          if (attempts == maxRetries) {
            logger.error(s"Max retries reached for batch insert. Error: ${e.getMessage}")
            val subject = "Critical: Unexpected error occurred during batch insert"
            val messageText = s"Unexpected error during batch insert: ${e.getMessage}. All retry attempts failed."
            EmailAlert.sendEmailAlert(subject, messageText, toEmail, smtpHost, smtpPort, smtpUser, smtpPassword)
            sendToDeadLetterQueue(batchDF) // Send failed batch to Dead Letter Queue
          }
          backoffDuration = backoffDuration * 2
          Thread.sleep(backoffDuration.toMillis)
      }
    }
  }

  def sendToDeadLetterQueue(batchDF: DataFrame): Unit = {
    val timestamp = System.currentTimeMillis()
    val deadLetterDir = "/path/to/dead-letter-queue"  // Directory where failed records will be stored

    try {
      // Make sure the directory exists
      val path = java.nio.file.Paths.get(deadLetterDir)
      if (!java.nio.file.Files.exists(path)) {
        java.nio.file.Files.createDirectories(path)
      }

      // Create a unique filename for each failed batch based on the timestamp
      val fileName = s"failed_batch_$timestamp.json"
      val filePath = s"$deadLetterDir/$fileName"

      // Convert the DataFrame to JSON and save it to the dead letter queue directory
      val failedBatchJson = batchDF
        .withColumn("timestamp", lit(timestamp))  // Add timestamp column for tracking
        .selectExpr("CAST(message AS STRING)", "timestamp")  // Assuming 'message' is the column that failed
        .toJSON

      // Write failed records to a file in the dead letter queue
      failedBatchJson.write
        .mode("append")
        .json(filePath)

      logger.error(s"Failed batch written to dead letter queue at: $filePath")

    } catch {
      case e: Exception =>
        logger.error(s"Error writing to dead letter queue: ${e.getMessage}")
    }
  }
}
