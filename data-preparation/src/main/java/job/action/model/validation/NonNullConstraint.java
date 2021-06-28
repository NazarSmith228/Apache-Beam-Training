package job.action.model.validation;

import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;

import java.io.Serializable;

public class NonNullConstraint implements ValidationConstraint, Serializable {

    @Override
    public boolean validate(CsvGenericRecord input, ConfigActions.Validate action) {
        return checkEmptiness(input, action);
    }
}
