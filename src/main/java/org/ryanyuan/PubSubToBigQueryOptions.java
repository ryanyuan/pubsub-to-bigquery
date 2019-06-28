package org.ryanyuan;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface PubSubToBigQueryOptions extends DataflowPipelineOptions {
    @Description("Pub/Sub topic name.")
    @Default.String("topic")
    String getPubSubTopic();
    void setPubSubTopic(String pubSubTopic);

    @Description("BigQuery dataset name.")
    @Default.String("dataset")
    String getBigQueryDataset();
    void setBigQueryDataset(String bigQueryDataset);

    @Description("BigQuery table name.")
    @Default.String("table")
    String getBigQueryTable();
    void setBigQueryTable(String bigQueryTable);
}