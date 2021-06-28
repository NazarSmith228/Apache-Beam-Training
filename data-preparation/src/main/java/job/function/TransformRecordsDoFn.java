package job.function;

import job.config.utils.ConfigHandler;
import job.model.common.CsvGenericRecord;
import job.model.config.RecordLayout;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.*;
import java.util.stream.Collectors;

public class TransformRecordsDoFn extends DoFn<String, CsvGenericRecord> {

    @ProcessElement
    public void processRecord(@Element String row, OutputReceiver<CsvGenericRecord> receiver) {
        Map<String, RecordLayout> configLayouts = ConfigHandler.getConfigLayouts();

        List<String> recordColumns = Arrays.stream(row.split(",")).collect(Collectors.toList());
        String recordType = recordColumns.get(0);
        recordColumns.remove(recordType);

        recordColumns = recordColumns.stream()
                .map(String::trim)
                .collect(Collectors.toList());

        RecordLayout recordLayouts = configLayouts.get(recordType);

        //skip if the record is unknown
        if (recordLayouts == null) {
            return;
        }
        List<String> fieldNames = new ArrayList<>(recordLayouts.getFieldTypes().keySet());

        CsvGenericRecord csvGenericRecord = new CsvGenericRecord();
        csvGenericRecord.setRecordType(recordType);
        csvGenericRecord.setRecordLayout(recordLayouts);

        Map<String, String> recordFields = new HashMap<>();

        for (int i = 0; i < recordColumns.size(); i++) {
            String column = recordColumns.get(i);
            recordFields.put(fieldNames.get(i), column);
        }

        csvGenericRecord.setFields(recordFields);
        receiver.output(csvGenericRecord);
    }

}
