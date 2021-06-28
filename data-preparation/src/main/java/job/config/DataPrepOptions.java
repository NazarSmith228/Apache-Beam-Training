package job.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface DataPrepOptions extends PipelineOptions {

    @Description("Path of the file to read data from")
    @Validation.Required
    String getInputPath();

    void setInputPath(String val);

    @Description("Path of the file to write data to")
    @Validation.Required
    String getOutputPath();

    void setOutputPath(String val);

    @Description("Path of the file where the AVRO schema will be stored")
    @Validation.Required
    String getSchemaPath();

    void setSchemaPath(String val);

    @Description("Flag responsible for debugging mode")
    @Default.Boolean(value = true)
    Boolean getDebugMode();

    void setDebugMode(Boolean flag);

    @Description("Directory path, where all mediate results are stored")
    String getDebugPath();

    void setDebugPath(String val);

    @Description("Path to file with .yaml configuration for layouts and actions")
    @Validation.Required
    String getConfigPath();

    void setConfigPath(String val);
}
