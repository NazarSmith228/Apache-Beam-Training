package job.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface WordOptions extends PipelineOptions {

    @Description("Path of the file to read data from")
    @Validation.Required
    String getInput();

    void setInput(String val);

    @Description("Path of the file to write data to")
    @Validation.Required
    String getOutput();

    void setOutput(String val);

    @Description("Path of the file where the AVRO schema will be stored")
    @Default.String(value = "word-aggregator/src/main/resources/schema/generated_schema")
    String getSchemaPath();

    void setSchemaPath(String val);

    @Description("Flag responsible for debugging mode")
    @Default.Boolean(value = true)
    Boolean getDebugMode();

    void setDebugMode(Boolean flag);

    @Description("Directory path, where all mediate results are stored")
    @Default.String(value = "word-aggregator/src/main/resources/debug")
    String getDebugPath();

    void setDebugPath(String val);
}
