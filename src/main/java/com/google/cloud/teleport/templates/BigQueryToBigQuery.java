package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.StreamingOptions;
import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;

/**
 * An example that reads the public samples of weather data from BigQuery, counts the number of
 * tornadoes that occur in each month, and writes the results to BigQuery.
 *
 * <p>Concepts: Reading/writing BigQuery; counting a PCollection; user-defined PTransforms
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p>To execute this pipeline locally, specify the BigQuery table for the output with the form:
 *
 * <pre>{@code
 * --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>The BigQuery input table defaults to {@code clouddataflow-readonly:samples.weather_stations}
 * and can be overridden with {@code --input}.
 */
public class BigQueryToBigQuery {
    // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
    private static final String SAMPLES_TABLE =
            "unity-analytics-exp-plt-test:bnames.names2010";

    /**
     * Examines each row in the input table. If a tornado was recorded in that sample, the month in
     * which it occurred is output.
     */
    static class ExtractGenderFn extends DoFn<TableRow, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            if ( ((String)row.get("gender")).length() > 0 ){
                c.output((String) row.get("gender"));
            }
        }
    }

    /**
     * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
     * representation of month and the number of tornadoes that occurred in each month.
     */
    static class FormatCountsFn extends DoFn<KV<String, Long>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row =
                    new TableRow()
                            .set("gender", c.element().getKey())
                            .set("count", c.element().getValue());
            c.output(row);
        }
    }

    /**
     * Takes rows from a table and generates a table of counts.
     *
     * <p>The input schema is described by https://developers.google.com/bigquery/docs/dataset-gsod .
     * The output contains the total number of tornadoes found in each month in the following schema:
     *
     * <ul>
     *   <li>month: integer
     *   <li>tornado_count: integer
     * </ul>
     */
    static class CountNames extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> rows) {

            // row... => month...
            PCollection<String> genders = rows.apply(ParDo.of(new ExtractGenderFn()));

            // month... => <month,count>...
            PCollection<KV<String, Long>> genderCounts = genders.apply(Count.perElement());

            // <month,count>... => row...
            PCollection<TableRow> results = genderCounts.apply(ParDo.of(new FormatCountsFn()));

            return results;
        }
    }

    /**
     * Options supported by {@link BigQueryTornadoes}.
     *
     * <p>Inherits standard configuration options.
     */
    public interface Options extends PipelineOptions,StreamingOptions {
        @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
        @Default.String(SAMPLES_TABLE)
        String getInput();

        void setInput(String value);

        @Description(
                "BigQuery table to write to, specified as "
                        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

        @Description("The window duration in which data will be written. Defaults to 5m. "
                + "Allowed formats are: "
                + "Ns (for seconds, example: 5s), "
                + "Nm (for minutes, example: 12m), "
                + "Nh (for hours, example: 2h).")
        @Default.String("15m")
        String getWindowDuration();
        void setWindowDuration(String value);
    }

    static void runBigQueryTornadoes(Options options) {
        Pipeline p = Pipeline.create(options);

        // Build the table schema for the output table.
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("gender").setType("STRING"));
        fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
        TableSchema schema = new TableSchema().setFields(fields);

        p.apply(BigQueryIO.readTableRows().from(options.getInput()))
                .apply(new CountNames())
                .apply(
                        BigQueryIO.writeTableRows()
                                .to(options.getOutput())
                                .withSchema(schema)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runBigQueryTornadoes(options);
    }
}