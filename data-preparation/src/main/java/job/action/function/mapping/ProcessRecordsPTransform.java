package job.action.function.mapping;

import job.action.function.mapping.dofn.ConvertToGenericRecordsDoFn;
import job.action.model.mapping.RowMapper;
import job.action.model.mapping.coder.GenericRecordCoder;
import job.config.DataPrepOptions;
import job.model.common.CsvGenericRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class ProcessRecordsPTransform extends PTransform<PCollection<CsvGenericRecord>, PCollection<GenericRecord>> {

    private final String fileName;
    private final String jsonSchema;

    public ProcessRecordsPTransform(String fileName, String jsonSchema) {
        this.fileName = fileName;
        this.jsonSchema = jsonSchema;
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<CsvGenericRecord> input) {
        DataPrepOptions options = input.getPipeline()
                .getOptions()
                .as(DataPrepOptions.class);

        PCollection<Row> rows = input
                .apply("Map CSV records to Rows",
                        ParDo.of(
                                new DoFn<CsvGenericRecord, Row>() {
                                    @ProcessElement
                                    public void process(@Element CsvGenericRecord element, OutputReceiver<Row> receiver) {
                                        receiver.output(RowMapper.mapToRow(element));
                                    }
                                }
                        )
                )
                .setCoder(SerializableCoder.of(Row.class));

        PCollection<GenericRecord> genericRecords = rows
                .apply("Map Rows to GenericRecords",
                        ParDo.of(new ConvertToGenericRecordsDoFn())
                )
                .setCoder(GenericRecordCoder.of());

        genericRecords
                .apply("Write records to Avro",
                        AvroIO.writeGenericRecords(jsonSchema)
                                .to(options.getOutputPath() + "/" + fileName)
                                .withoutSharding());

        return genericRecords;
    }
}
