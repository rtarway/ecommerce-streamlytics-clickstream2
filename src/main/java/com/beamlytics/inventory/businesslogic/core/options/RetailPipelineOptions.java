package com.beamlytics.inventory.businesslogic.core.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface RetailPipelineOptions
    extends
    DataflowPipelineOptions,
    RetailPipelineAggregationOptions,
    RetailPipelineClickStreamOptions,
    RetailPipelineInventoryOptions,
    RetailPipelineTransactionsOptions,
    RetailPipelineStoresOptions,
    RetailPipelineReportingOptions {

  @Default.Boolean(false)
  Boolean getDebugMode();

  void setDebugMode(Boolean debugMode);

  @Default.Boolean(false)
  Boolean getTestModeEnabled();

  void setTestModeEnabled(Boolean testModeEnabled);

  // Redis Options
  @Description("Redis Host")
  @Default.String("127.0.0.1")
  String getRedisHost();

  void setRedisHost(String value);

  @Description("Redis Port")
  @Default.Integer(6379)
  Integer getRedisPort();

  void setRedisPort(Integer value);

  // Batch Input Options
  @Description("Path to Clickstream Batch File (JSON)")
  String getClickstreamBatchFile();

  void setClickstreamBatchFile(String value);

  @Description("Path to Stock Batch File (JSON)")
  String getStockBatchFile();

  void setStockBatchFile(String value);

  @Description("Path to Transaction Batch File (JSON)")
  String getTransactionBatchFile();

  void setTransactionBatchFile(String value);
}
