package job.action.model.mapping.factory;

import org.apache.beam.sdk.schemas.Schema;

import java.util.Map;

public class SchemaFactory {

    public static Schema createSchema(Map<String, String> innerRecordFieldTypes) {
        Schema.Builder innerBuilder = Schema.builder();
        innerRecordFieldTypes.forEach(
                (key, value) -> innerBuilder.addField(key, resolveFieldType(value))
        );

        return innerBuilder.build();
    }

    private static Schema.FieldType resolveFieldType(String fieldType) {
        switch (fieldType) {
            case "string":
                return Schema.FieldType.STRING;
            case "byte":
                return Schema.FieldType.BYTE;
            case "short":
                return Schema.FieldType.INT16;
            case "int":
                return Schema.FieldType.INT32;
            case "long":
                return Schema.FieldType.INT64;
            case "double":
                return Schema.FieldType.DOUBLE;
            case "float":
                return Schema.FieldType.FLOAT;
            case "boolean":
                return Schema.FieldType.BOOLEAN;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + fieldType);

        }
    }
}
