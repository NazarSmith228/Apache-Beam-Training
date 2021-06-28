package job.action.model.mapping.factory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowFactory {

    public static Row createInnerRow(Schema rowSchema, Map<String, String> innerRecordFieldValues) {
        return Row.withSchema(rowSchema)
                .addValues(constructRowValues(rowSchema, innerRecordFieldValues))
                .build();
    }

    private static List<Object> constructRowValues(Schema recordSchema, Map<String, String> innerRecordFieldValues) {
        List<Object> rowValues = new ArrayList<>();
        List<Schema.Field> rowFields = recordSchema.getFields();

        for (Schema.Field rowField : rowFields) {
            String fieldName = rowField.getName();
            String fieldValue = innerRecordFieldValues.get(fieldName);
            Schema.FieldType fieldType = rowField.getType();
            rowValues.add(resolveFieldValue(fieldType, fieldValue));
        }

        return rowValues;
    }

    private static Object resolveFieldValue(Schema.FieldType fieldType, String fieldValue) {
        if (fieldType.equals(Schema.FieldType.STRING)) {
            return fieldValue;
        } else if (fieldType.equals(Schema.FieldType.BYTE)) {
            return Byte.parseByte(fieldValue);
        } else if (fieldType.equals(Schema.FieldType.INT16)) {
            return Short.parseShort(fieldValue);
        } else if (fieldType.equals(Schema.FieldType.INT32)) {
            return Integer.parseInt(fieldValue);
        } else if (fieldType.equals(Schema.FieldType.INT64)) {
            return Long.parseLong(fieldValue);
        } else if (fieldType.equals(Schema.FieldType.DOUBLE)) {
            return Double.parseDouble(fieldValue);
        } else if (fieldType.equals(Schema.FieldType.FLOAT)) {
            return Float.parseFloat(fieldValue);
        } else if (fieldType.equals(Schema.FieldType.BOOLEAN)) {
            return Boolean.parseBoolean(fieldValue);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
