package com.peppysisay

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.{FileIO}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.{Duration, Instant}

import scala.io.Source

object ChatAnalysisPipeline {

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[ChatAnalysisPipelineOptions])

    val inputSchemaJson = Source.fromFile(options.getInputSchemaPath()).mkString
    val outputSchemaJson = Source.fromFile(options.getOutputSchemaPath()).mkString

    val inputSchema = new Schema.Parser().parse(inputSchemaJson)
    val outputSchema = new Schema.Parser().parse(outputSchemaJson)

    val pipeline = Pipeline.create(options)

    val input =
      pipeline
        .apply(PubsubIO.readMessages().fromSubscription(options.getPubsubSubscription()))
        .apply(ParDo.of(new ValidatePubsubMessageSchema(inputSchemaJson)))
        .setCoder(AvroCoder.of(inputSchema))

    // Handle input/output in windows
    val windowedInput =
      input.apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))))

    windowedInput.apply(
      FileIO.write()
        .via(ParquetIO.sink(outputSchema))
        .to(options.getOutput())
        .withSuffix(".parquet")
        .withNumShards(1)
    )

    println("Running pipeline...")

    val result = pipeline.run()

    try {
      result.waitUntilFinish()
    } catch {
      case exc: Exception =>
        println("Errored out")
        print(exc.getMessage)
        result.cancel()
    }
  }
}

class ValidatePubsubMessageSchema(schemaJson: String) extends DoFn[PubsubMessage, GenericRecord] {
  @ProcessElement
  def processElement(context: ProcessContext): Unit = {
    println("RECEIVED MESSAGE: " + new String(context.element().getPayload()))

    val schema = new Schema.Parser().parse(schemaJson)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get.binaryDecoder(context.element().getPayload(), null)
    val record = reader.read(null, decoder)

    context.outputWithTimestamp(record, new Instant())
  }
}

trait ChatAnalysisPipelineOptions extends PipelineOptions {

  @Description("Pubsub subscription")
  @Default.String("projects/personal-site-staging-a449f/subscriptions/group-chat-message-data-flow")
  def getPubsubSubscription(): String

  def setPubsubSubscription(topic: String)

  @Description("Path to input JSON schema")
  @Required
  def getInputSchemaPath(): String

  def setInputSchemaPath(schema: String)

  @Description("Path to output JSON schema")
  @Required
  def getOutputSchemaPath(): String

  def setOutputSchemaPath(schema: String)

  @Description("Path of the file to write to")
  @Default.String("gs://worship-team-chat-bot")
  def getOutput(): String

  def setOutput(path: String)

  @Description("Duration for window (minutes)")
  @Default.Integer(1)
  def getWindowSize(): Int

  def setWindowSize(size: Int)
}
