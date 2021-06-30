package job.model.config;

import lombok.Builder;
import lombok.Data;
import org.apache.beam.sdk.schemas.Schema;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class ConfigActions implements Serializable {
    private List<Validate> validateActions;
    private List<GroupBy> groupingActions;
    private Map<String, MapToAvro> mappingActions;

    @Data
    @Builder
    public static class Validate implements Serializable {
        private String recordType;
        private String fieldName;
        private String validationConstraint;
    }

    @Data
    @Builder
    public static class GroupBy implements Serializable {
        private String resultRecordName;
        private String groupingKey;
        private List<String> recordTypes;
    }

    @Data
    @Builder
    public static class MapToAvro implements Serializable {
        private String targetSchema;
        private Map<String, String> fieldsMapping;
        private Schema avroSchema;
    }
}
