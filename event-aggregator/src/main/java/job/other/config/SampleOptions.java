package job.other.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface SampleOptions extends PipelineOptions {

    @Description("Path of the file to read data from")
    @Default.String(value = "event-aggregator/src/main/resources/source/*")
    String getInput();

    void setInput(String val);

    @Description("Path of the file to write data to")
    @Default.String(value = "event-aggregator/src/main/resources/other/aggregated_data")
    @Validation.Required
    String getOutput();

    void setOutput(String val);

}
