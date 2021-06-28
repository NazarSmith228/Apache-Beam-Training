package job.action.function.grouping.dofn;

import job.model.common.CsvGenericRecord;
import job.model.config.RecordLayout;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ComposeGroupedRecordsDoFn extends DoFn<KV<String, Iterable<CsvGenericRecord>>, CsvGenericRecord> {

    private final String resultRecordName;
    private final RecordLayout resultRecordLayout;

    public ComposeGroupedRecordsDoFn(String resultRecordName, RecordLayout resultRecordLayout) {
        this.resultRecordName = resultRecordName;
        this.resultRecordLayout = resultRecordLayout;
    }

    @ProcessElement
    public void process(@Element KV<String, Iterable<CsvGenericRecord>> element, OutputReceiver<CsvGenericRecord> receiver) {
        Iterable<CsvGenericRecord> csvGenericRecords = element.getValue();
        List<CsvGenericRecord> genericRecordList = StreamSupport.stream(csvGenericRecords.spliterator(), false)
                .collect(Collectors.toList());

        Map<String, String> resultRecordFields = new HashMap<>();
        genericRecordList.forEach(
                record -> {
                    Map<String, String> recordFields = record.getFields();
                    recordFields.forEach(resultRecordFields::putIfAbsent);
                }
        );

        CsvGenericRecord resultRecord = CsvGenericRecord.builder()
                .recordType(resultRecordName)
                .recordLayout(resultRecordLayout)
                .fields(resultRecordFields)
                .build();

        receiver.output(resultRecord);
    }
}
