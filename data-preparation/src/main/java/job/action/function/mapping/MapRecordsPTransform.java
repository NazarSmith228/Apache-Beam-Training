package job.action.function.mapping;

import job.action.model.mapping.coder.GenericRecordCoder;
import job.config.utils.ConfigHandler;
import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Map;
import java.util.Set;

public class MapRecordsPTransform extends PTransform<PCollection<CsvGenericRecord>, PCollection<GenericRecord>> {

    private final Map<String, ConfigActions.MapToAvro> mappingActions;

    public MapRecordsPTransform() {
        this.mappingActions = ConfigHandler.getConfigActions().getMappingActions();
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<CsvGenericRecord> input) {
        PCollectionList<GenericRecord> resultList = PCollectionList.empty(input.getPipeline());

        Set<String> targetRecordTypes = mappingActions.keySet();
        for (String targetRecordType : targetRecordTypes) {
            PCollection<CsvGenericRecord> targetRecordsToMap = input
                    .apply("Filter records by target mapping type",
                            Filter.by(
                                    record -> record.getRecordType().equals(targetRecordType)
                            )
                    )
                    .setCoder(SerializableCoder.of(CsvGenericRecord.class));

            ConfigActions.MapToAvro mappingAction = mappingActions.get(targetRecordType);
            String targetSchema = mappingAction.getTargetSchema();
            String avroSchema = AvroUtils.toAvroSchema(mappingAction.getAvroSchema()).toString();

            PCollection<GenericRecord> genericRecords = targetRecordsToMap
                    .apply("Map CSV records to GenericRecord.class and write the result to Avro",
                            new ProcessRecordsPTransform(targetSchema, avroSchema)
                    );

            resultList = resultList.and(genericRecords);
        }

        return resultList
                .apply("Merge all resulting PCollections",
                        Flatten.pCollections()
                )
                .setCoder(GenericRecordCoder.of());
    }
}
