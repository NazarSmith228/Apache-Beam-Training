package job.action.model.validation;

import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;

import java.util.Map;

public interface ValidationConstraint {

    boolean validate(CsvGenericRecord input, ConfigActions.Validate action);

    default boolean checkEmptiness(CsvGenericRecord input, ConfigActions.Validate action) {
        Map<String, String> fields = input.getFields();
        String fieldName = action.getFieldName();

        return fields.get(fieldName) != null && !fields.get(fieldName).equals("null") && !fields.get(fieldName).isEmpty();
    }
}
