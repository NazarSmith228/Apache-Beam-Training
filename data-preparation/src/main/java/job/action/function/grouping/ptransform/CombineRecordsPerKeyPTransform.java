package job.action.function.grouping.ptransform;

import job.model.common.CsvGenericRecord;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.stream.StreamSupport;

public class CombineRecordsPerKeyPTransform extends PTransform<PCollection<KV<String, CsvGenericRecord>>,
        PCollection<KV<String, Iterable<CsvGenericRecord>>>> {

    private final int numberOfRecordsToGroup;

    public CombineRecordsPerKeyPTransform(int numberOfRecordsToGroup) {
        this.numberOfRecordsToGroup = numberOfRecordsToGroup;
    }

    @Override
    public PCollection<KV<String, Iterable<CsvGenericRecord>>> expand(PCollection<KV<String, CsvGenericRecord>> input) {
        return input
                .apply("Group records by the assigned grouping key and filter the number of records per each key",
                        GroupByKey.create()
                )
                .apply("Filter number of records per key (shouldn`t be more than 2 records per key)",
                        Filter.by(
                                kv -> checkIterableRecords(kv.getValue())
                        )
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                IterableCoder.of(
                                        SerializableCoder.of(CsvGenericRecord.class)
                                )
                        )
                );
    }

    private boolean checkIterableRecords(Iterable<CsvGenericRecord> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .count() == numberOfRecordsToGroup;
    }
}
