package com.peppysisay

import com.peppysisay.avro.InputRecord
import com.peppysisay.avro.OutputRecord
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.{Duration, Instant}

object ChatAnalysisPipeline {

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[ChatAnalysisPipelineOptions])

    val pipeline = Pipeline.create(options)

    val input = pipeline
      .apply("ReadPubsubMessages", PubsubIO.readMessages.fromSubscription(options.getPubsubSubscription()))
      .apply("ValidateMessages", ParDo.of(new DecodeAndValidatePubsubMessage()))

    val windowedWords =
      input.apply(
        Window.into(FixedWindows.of(Duration.standardMinutes(1))))

    windowedWords
      .apply("DebugMessages", ParDo.of(new DebugMessage()))
      .setCoder(AvroCoder.of(OutputRecord.getClassSchema))
      .apply("Output",
        FileIO.write[GenericRecord]()
          .via(ParquetIO.sink(OutputRecord.getClassSchema))
          .to("/output")
          .withSuffix(".json")
          .withNumShards(1)
      )

    println("Running pipeline...")

    val result = pipeline.run()

    try {
      result.waitUntilFinish()
    } catch {
      case exception: Exception => {
        println("Errored out")
        print(exception.printStackTrace())
        result.cancel()
      }
    }
  }
}

class DecodeAndValidatePubsubMessage extends DoFn[PubsubMessage, InputRecord] {
  @ProcessElement
  def processElement(context: ProcessContext): Unit = {
    println("Handling pubsub message " + new String(context.element().getPayload))

    try {
      val reader = new SpecificDatumReader[InputRecord](InputRecord.getClassSchema)
      val decoder = DecoderFactory.get.jsonDecoder(InputRecord.getClassSchema, new String(context.element().getPayload))
      val record = reader.read(null, decoder);

      context.outputWithTimestamp(record, new Instant())
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
  }
}


class DebugMessage extends DoFn[GenericRecord, GenericRecord] {
  @ProcessElement
  def processElement(context: ProcessContext): Unit = {
    println("About to output message " + new String(context.element().toString))

    context.output(context.element())
  }
}

trait ChatAnalysisPipelineOptions extends PipelineOptions {

  @Description("Pubsub subscription")
  @Default.String("projects/personal-site-staging-a449f/subscriptions/group-chat-message-data-flow")
  def getPubsubSubscription(): String

  def setPubsubSubscription(topic: String)

  @Description("Path of the file to write to")
  @Default.String("sdfsdfsdfsdfs")
  def getOutput(): String

  def setOutput(path: String)

  @Description("Duration for window (minutes)")
  @Default.Integer(1)
  def getWindowSize(): Int

  def setWindowSize(size: Int)
}
