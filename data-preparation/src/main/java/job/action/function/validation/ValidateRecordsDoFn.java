package job.action.function.validation;

import job.config.utils.ConfigHandler;
import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import job.action.model.validation.ValidationConstraint;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ValidateRecordsDoFn extends DoFn<CsvGenericRecord, CsvGenericRecord> {

    private final List<ConfigActions.Validate> validationActions;

    public ValidateRecordsDoFn() {
        this.validationActions = ConfigHandler.getConfigActions().getValidateActions();
    }

    @ProcessElement
    public void processRecord(@Element CsvGenericRecord input, OutputReceiver<CsvGenericRecord> receiver) {
        String recordType = input.getRecordType();
        List<ConfigActions.Validate> matchedActions = validationActions.stream()
                .filter(action -> action.getRecordType().equals(recordType))
                .collect(Collectors.toList());

        AtomicBoolean passed = new AtomicBoolean(true);
        matchedActions.forEach(
                action -> {
                    String validationConstraintString = action.getValidationConstraint();
                    try {
                        Class<?> constraintClass = Class.forName(validationConstraintString);
                        ValidationConstraint validationConstraint = (ValidationConstraint) constraintClass.newInstance();

                        if (!validationConstraint.validate(input, action)) {
                            passed.set(false);
                        }
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                        throw new RuntimeException("An exception occurred during validation of: " + input + ".\nException: " + e);
                    }
                }
        );

        if (passed.get()) {
            receiver.output(input);
        }
    }
}
