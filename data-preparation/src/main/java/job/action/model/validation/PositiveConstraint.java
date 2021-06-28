package job.action.model.validation;

import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import job.model.config.RecordLayout;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PositiveConstraint implements ValidationConstraint, Serializable {

    private static List<String> numberTypes;

    public PositiveConstraint() {
        initNumberTypes();
    }

    private static void initNumberTypes() {
        numberTypes = Arrays.asList(
                "byte",
                "short",
                "int",
                "long",
                "double",
                "float"
        );
    }

    @Override
    public boolean validate(CsvGenericRecord input, ConfigActions.Validate action) {
        Map<String, String> fields = input.getFields();
        String fieldName = action.getFieldName();

        if (!checkEmptiness(input, action)) {
            return false;
        }

        RecordLayout recordLayout = input.getRecordLayout();
        String fieldType = recordLayout.getFieldTypes().get(fieldName);

        if (!numberTypes.contains(fieldType)) {
            //throw exception or simply skip the record?
            return false;
        }

        return isPositive(fields.get(fieldName));
    }

    private boolean isPositive(String input) {
        BigDecimal bigDecimal;
        try {
            bigDecimal = new BigDecimal(input, MathContext.UNLIMITED);
        } catch (NumberFormatException ex) {
            return false;
        }
        return bigDecimal.compareTo(BigDecimal.ZERO) > 0;
    }
}
