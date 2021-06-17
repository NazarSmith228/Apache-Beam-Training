package aggregator.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface JobOptions extends PipelineOptions {

    @Description("Path of the file to read data from")
    @Validation.Required
    String getInput();

    void setInput(String val);

    @Description("Path of the file to write data to")
    @Validation.Required
    String getOutput();

    void setOutput(String val);

    @Description("Flag responsible for debugging mode")
    Boolean getDebugMode();

    void setDebugMode(Boolean flag);
}
