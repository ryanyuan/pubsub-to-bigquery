package org.ryanyuan;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.ryanyuan.transforms.JsonMsgToTableRow;


import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

/**
 * Dataflow streaming pipeline to read messages from PubSub and write the
 * payload to BigQuery
 */
public class PubSubToBigQueryPipeline {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PubSubToBigQueryOptions.class);
        PubSubToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        run(options);

    }
    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(PubSubToBigQueryOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<PubsubMessage> messages;

        if (options.getUseSubscription()) {
            messages = pipeline
                    .apply("Read from PubSub", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription()));
        } else {
            messages = pipeline
                    .apply("Read from PubSub", PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
        }

        messages.apply("Convert JSON to row", new JsonMsgToTableRow())
                .apply("Write to BigQuery",
                        BigQueryIO.writeTableRows()
                                .withExtendedErrorInfo()
                                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                .withSchema(JsonMsgToTableRow.getSchema())
                                .to(options.getOutputTableSpec())
                                .withoutValidation()
                                .withCreateDisposition(CREATE_IF_NEEDED)
                                .withWriteDisposition(WRITE_APPEND));

        return pipeline.run();
    }
}