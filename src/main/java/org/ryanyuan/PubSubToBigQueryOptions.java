package org.ryanyuan;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubSubToBigQueryOptions extends DataflowPipelineOptions {
    @Description("Table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();
    void setOutputTableSpec(ValueProvider<String> value);

    @Description("Pub/Sub topic to read the input from")
    ValueProvider<String> getInputTopic();
    void setInputTopic(ValueProvider<String> value);

    @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();
    void setInputSubscription(ValueProvider<String> value);

    @Description(
            "This determines whether the template reads from " + "a pub/sub subscription or a topic")
    @Default.Boolean(true)
    Boolean getUseSubscription();
    void setUseSubscription(Boolean value);
}