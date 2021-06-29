package job.action.function.mapping.dofn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class ConvertToGenericRecordsDoFn extends DoFn<Row, GenericRecord> {

    @ProcessElement
    public void process(@Element Row element, OutputReceiver<GenericRecord> receiver) {
        Schema rowSchema = element.getSchema();

        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(rowSchema);
        GenericRecord outputRecord = AvroUtils.toGenericRecord(element, avroSchema);

        receiver.output(outputRecord);
    }
}
