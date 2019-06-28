package org.ryanyuan.transforms;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JsonMsgToTableRow
        extends PTransform<PCollection<PubsubMessage>, PCollection<TableRow>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMsgToTableRow.class);

    private static final String TYPE_STRING = "STRING";
    private static final String TYPE_FLOAT = "FLOAT";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_GEO = "geo";
    private static final String FIELD_BENSIN95 = "bensin95";
    private static final String FIELD_DIESEL = "diesel";

    @Override
    public PCollection<TableRow> expand(PCollection<PubsubMessage> stringPCollection) {
        return stringPCollection.apply("JsonToTableRow", MapElements.<PubsubMessage, TableRow>via(
                new SimpleFunction<PubsubMessage, TableRow>() {
                    @Override
                    public TableRow apply(PubsubMessage message) {
                        String json = new String(message.getPayload(), StandardCharsets.UTF_8);
                        LOGGER.info(json);
                        TableRow row = new TableRow();
                        try {
                            JSONObject jsonObj = new JSONObject(json);
                            row.set(FIELD_NAME, jsonObj.getString(FIELD_NAME));
                            row.set(FIELD_GEO, jsonObj.getJSONObject(FIELD_GEO).toString());
                            row.set(FIELD_BENSIN95, jsonObj.getFloat(FIELD_BENSIN95));
                            row.set(FIELD_DIESEL, jsonObj.getFloat(FIELD_DIESEL));
                            return row;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return row;
                    }
                }));
    }

    public static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName(FIELD_NAME).setType(TYPE_STRING));
        fields.add(new TableFieldSchema().setName(FIELD_GEO).setType(TYPE_STRING));
        fields.add(new TableFieldSchema().setName(FIELD_BENSIN95).setType(TYPE_FLOAT));
        fields.add(new TableFieldSchema().setName(FIELD_DIESEL).setType(TYPE_FLOAT));
        return new TableSchema().setFields(fields);
    }
}