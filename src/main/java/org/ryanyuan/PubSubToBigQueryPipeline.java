package org.ryanyuan;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.ryanyuan.transforms.JsonMsgToTableRow;


import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

/**
 * Dataflow streaming pipeline to read messages from PubSub subscription and write the
 * payload to BigQuery
 */
public class PubSubToBigQueryPipeline {
    private static final String SUBSCRIPTION = "projects/%s/subscriptions/%s";
    private static final String BIGQUERY_DATASET_TABLE = "%s:%s.%s";

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PubSubToBigQueryOptions.class);
        PubSubToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<PubsubMessage> messages = pipeline
                .apply("GetDataFromPubSub", PubsubIO.readMessagesWithAttributes().fromSubscription(String.format(SUBSCRIPTION, options.getProject(), options.getPubSubSubscription())))
                .apply("ApplyWindow", Window.into(FixedWindows.of(Duration.standardSeconds(1))));

        PCollection<TableRow> rows = messages.apply("JsonToRow", new JsonMsgToTableRow());

        rows.apply("WriteToBigQuery", BigQueryIO.writeTableRows().to(String.format(BIGQUERY_DATASET_TABLE, options.getProject(), options.getBigQueryDataset(), options.getBigQueryTable())).withoutValidation()
                .withSchema(JsonMsgToTableRow.getSchema()).withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_APPEND));
        pipeline.run();
    }
}