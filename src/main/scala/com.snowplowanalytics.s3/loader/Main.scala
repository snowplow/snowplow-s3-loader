package com.snowplowanalytics.s3.loader

import java.util.concurrent.TimeUnit
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter.Percentile
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.regions
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit
import scala.concurrent.Future

object Main {

  val start = System.nanoTime()

  val metrics = new MetricRegistry()

  val Region = regions.Region.EU_CENTRAL_1

  val awsClient: CloudWatchAsyncClient =
    CloudWatchAsyncClient
      .builder()
      .region(Region)
      .build()

  val cloudWatchReporter: CloudWatchReporter =
    CloudWatchReporter
      .forRegistry(metrics, awsClient, "s3-loader")
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .filter(MetricFilter.ALL)
      .withPercentiles(Percentile.P75, Percentile.P99)
      .withOneMinuteMeanRate()
      .withFiveMinuteMeanRate()
      .withFifteenMinuteMeanRate()
      .withMeanRate()
      .withArithmeticMean()
      .withStdDev()
      .withStatisticSet()
      .withZeroValuesSubmission()
      .withReportRawCountValue()
      .withHighResolution()
      .withMeterUnitSentToCW(StandardUnit.BYTES)
      // .withJvmMetrics()
      .withGlobalDimensions(s"Region=${Region.toString}", "Instance=stage")
      .withDryRun()
      .build()

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
  Future(cloudWatchReporter.start(100, TimeUnit.MILLISECONDS))

  val latency = metrics.histogram("some.name.foo")

  for (_ <- 0 until 100000) {
    latency.update(System.nanoTime() - start)
  }

  Thread.sleep(60 * 1000)

  import collection.JavaConverters._
  println(metrics.getHistograms().asScala)
  println(s"hist count: ${metrics.getHistograms().asScala.head._2.getCount()}")

}
