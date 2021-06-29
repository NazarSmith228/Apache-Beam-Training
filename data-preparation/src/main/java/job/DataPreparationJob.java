package job;

import job.action.function.grouping.ptransform.GroupRecordsPTransform;
import job.action.function.mapping.MapRecordsPTransform;
import job.action.function.validation.ValidateRecordsDoFn;
import job.action.model.mapping.coder.GenericRecordCoder;
import job.config.DataPrepOptions;
import job.config.utils.ConfigHandler;
import job.function.TransformRecordsDoFn;
import job.model.common.CsvGenericRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class DataPreparationJob {

    public static void main(String[] args) {
        DataPrepOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .withoutStrictParsing()
                .as(DataPrepOptions.class);

        ConfigHandler.loadConfig(options.getConfigPath());

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> rawData = pipeline.apply("Read data from .csv",
                TextIO.read()
                        .from(options.getInputPath())
        );

        PCollection<CsvGenericRecord> csvRecords = rawData
                .apply("Map to generic record POJO",
                        ParDo.of(
                                new TransformRecordsDoFn()
                        )
                )
                .setCoder(SerializableCoder.of(CsvGenericRecord.class));

        if (options.getDebugMode()) {
            writeMediateData(csvRecords, options, "/generic_records.txt");
        }

        PCollection<CsvGenericRecord> validatedRecords = csvRecords
                .apply("Validate all records",
                        ParDo.of(
                                new ValidateRecordsDoFn()
                        )
                )
                .setCoder(SerializableCoder.of(CsvGenericRecord.class));

        if (options.getDebugMode()) {
            writeMediateData(validatedRecords, options, "/validated_records.txt");
        }

        PCollection<CsvGenericRecord> groupedRecords = validatedRecords
                .apply("Group all valid records",
                        new GroupRecordsPTransform()
                )
                .setCoder(SerializableCoder.of(CsvGenericRecord.class));

        if (options.getDebugMode()) {
            writeMediateData(groupedRecords, options, "/grouped_records.txt");
        }

        PCollection<GenericRecord> mappedRecords = groupedRecords
                .apply("Map grouped records to AVRO format",
                        new MapRecordsPTransform()
                )
                .setCoder(GenericRecordCoder.of());

        mappedRecords.apply("Write results to Avro",
                FileIO.<String, GenericRecord>writeDynamic()
                        .by(record -> (String) record.get("targetSchema"))
                        .withDestinationCoder(StringUtf8Coder.of())
                        .via(AvroIO.sink(GenericRecord.class))
                        .to(options.getOutputPath())
                        .withNaming(key -> FileIO.Write.defaultNaming(key, ".avro")));

        pipeline.run().waitUntilFinish();
    }

    private static void writeMediateData(PCollection<?> pCollection, DataPrepOptions options, String suffix) {
        pCollection
                .apply("Map mediate processed data to strings",
                        MapElements.into(
                                TypeDescriptors.strings()
                        ).via(
                                Object::toString
                        )
                )
                .apply("Write mediate processed data to file",
                        TextIO.write()
                                .to(options.getDebugPath())
                                .withoutSharding()
                                .withSuffix(suffix)
                );
    }
}
