package com.peppysisay

import com.peppysisay.avro.InputRecord
import com.peppysisay.avro.OutputRecord
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.parquet.ParquetIO
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.Duration

object ChatAnalysisPipeline {

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[ChatAnalysisPipelineOptions])

    val pipeline = Pipeline.create(options)

    pipeline
      .apply(PubsubIO.readMessages().fromSubscription(options.getPubsubSubscription()))
      .apply(ParDo.of(new DecodeAndValidatePubsubMessage()))
      .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
      .apply(ParDo.of(new DebugMessage()))
      .apply(
        FileIO.write()
          .via(ParquetIO.sink(OutputRecord.getClassSchema()))
          .to(options.getOutput())
          .withSuffix(".parquet")
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

      context.output(record)
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
  @Default.String("projects/steam-aria-256204/subscriptions/data-flow")
  def getPubsubSubscription(): String

  def setPubsubSubscription(topic: String)

  @Description("Path of the file to write to")
  @Default.String("gs://peppy-dev-data-flow")
  def getOutput(): String

  def setOutput(path: String)

  @Description("Duration for window (minutes)")
  @Default.Integer(1)
  def getWindowSize(): Int

  def setWindowSize(size: Int)
}
