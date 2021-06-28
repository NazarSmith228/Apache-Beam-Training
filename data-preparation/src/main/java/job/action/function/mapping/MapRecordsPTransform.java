package job.action.function.mapping;

import job.action.function.mapping.dofn.ConvertToGenericRecordsDoFn;
import job.action.model.mapping.RowMapper;
import job.config.utils.ConfigHandler;
import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;

import java.util.Map;
import java.util.Set;

public class MapRecordsPTransform extends PTransform<PCollection<CsvGenericRecord>, PCollection<KV<String, GenericRecord>>> {

    private final Map<String, ConfigActions.MapToAvro> mappingActions;

    public MapRecordsPTransform() {
        this.mappingActions = ConfigHandler.getConfigActions().getMappingActions();
    }

    @Override
    public PCollection<KV<String, GenericRecord>> expand(PCollection<CsvGenericRecord> input) {
        PCollectionList<KV<String, GenericRecord>> resultList = PCollectionList.empty(input.getPipeline());

        Set<String> targetRecordTypes = mappingActions.keySet();
        for (String targetRecordType : targetRecordTypes) {
            PCollection<CsvGenericRecord> targetRecordsToMap = input
                    .apply("Filter records by target mapping type",
                            Filter.by(
                                    record -> record.getRecordType().equals(targetRecordType)
                            )
                    )
                    .setCoder(SerializableCoder.of(CsvGenericRecord.class));

            PCollection<KV<String, Row>> rows = targetRecordsToMap
                    .apply("Map CSV records to Rows",
                            ParDo.of(
                                    new DoFn<CsvGenericRecord, KV<String, Row>>() {
                                        @ProcessElement
                                        public void process(@Element CsvGenericRecord element, OutputReceiver<KV<String, Row>> receiver) {
                                            receiver.output(RowMapper.mapToRow(element));
                                        }
                                    }
                            )
                    )
                    .setCoder(
                            KvCoder.of(
                                    StringUtf8Coder.of(),
                                    SerializableCoder.of(Row.class)
                            )
                    );

            /**
             * apply {@link ConvertToGenericRecordsDoFn}
             */

            //add mediate collections to resultList
        }

        return resultList.apply(Flatten.pCollections());
    }
}
