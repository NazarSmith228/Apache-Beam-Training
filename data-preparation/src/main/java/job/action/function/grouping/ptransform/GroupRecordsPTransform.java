package job.action.function.grouping.ptransform;

import job.action.function.grouping.dofn.ComposeGroupedRecordsDoFn;
import job.config.utils.ConfigHandler;
import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import job.model.config.RecordLayout;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.List;
import java.util.Map;

public class GroupRecordsPTransform extends PTransform<PCollection<CsvGenericRecord>, PCollection<CsvGenericRecord>> {

    private final List<ConfigActions.GroupBy> groupingActions;
    private final Map<String, RecordLayout> layouts;

    public GroupRecordsPTransform() {
        this.groupingActions = ConfigHandler.getConfigActions().getGroupingActions();
        this.layouts = ConfigHandler.getConfigLayouts();
    }

    @Override
    public PCollection<CsvGenericRecord> expand(PCollection<CsvGenericRecord> input) {
        PCollectionList<CsvGenericRecord> resultList = PCollectionList.empty(input.getPipeline());

        for (ConfigActions.GroupBy groupingAction : groupingActions) {
            List<String> recordTypes = groupingAction.getRecordTypes();

            PCollection<CsvGenericRecord> matchedRecords = input
                    .apply("Extract records matching record types from grouping action",
                            Filter.by(
                                    record -> recordTypes.contains(record.getRecordType())
                            )
                    )
                    .setCoder(SerializableCoder.of(CsvGenericRecord.class));

            String groupingKey = groupingAction.getGroupingKey();

            PCollection<KV<String, CsvGenericRecord>> keyedRecords = matchedRecords
                    .apply("Assign a grouping key for each matched record",
                            ParDo.of(
                                    new DoFn<CsvGenericRecord, KV<String, CsvGenericRecord>>() {
                                        @ProcessElement
                                        public void process(ProcessContext ctx) {
                                            CsvGenericRecord record = ctx.element();
                                            Map<String, String> recordFields = record.getFields();

                                            ctx.output(KV.of(recordFields.get(groupingKey), record));
                                        }
                                    }
                            )
                    )
                    .setCoder(
                            KvCoder.of(
                                    StringUtf8Coder.of(),
                                    SerializableCoder.of(CsvGenericRecord.class)
                            )
                    );

            int numberOfRecordsToGroup = recordTypes.size();

            PCollection<KV<String, Iterable<CsvGenericRecord>>> groupedRecords = keyedRecords
                    .apply("Apply a PTransform which combines records per grouping key",
                            new CombineRecordsPerKeyPTransform(numberOfRecordsToGroup)
                    );

            String resultRecordName = groupingAction.getResultRecordName();
            RecordLayout resultRecordLayout = layouts.get(resultRecordName);

            PCollection<CsvGenericRecord> resultRecords = groupedRecords
                    .apply("Apply a DoFn which creates final composite records",
                            ParDo.of(new ComposeGroupedRecordsDoFn(resultRecordName, resultRecordLayout))
                    )
                    .setCoder(SerializableCoder.of(CsvGenericRecord.class));

            resultList = resultList.and(resultRecords);
        }

        return resultList.apply(Flatten.pCollections());
    }
}
