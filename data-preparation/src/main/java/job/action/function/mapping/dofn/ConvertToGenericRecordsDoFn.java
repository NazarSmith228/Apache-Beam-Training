package job.action.function.mapping.dofn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;

public class ConvertToGenericRecordsDoFn extends DoFn<KV<String,Row>, KV<String,GenericRecord>> {

    @ProcessElement
    public void process(@Element KV<String, Row> element, OutputReceiver<KV<String, GenericRecord>> receiver) {
        Row row = element.getValue();
        Schema rowSchema = row.getSchema();
        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(rowSchema);
        GenericRecord outputRecord = AvroUtils.toGenericRecord(row, avroSchema);

        receiver.output(KV.of(element.getKey(), outputRecord));
    }
}
