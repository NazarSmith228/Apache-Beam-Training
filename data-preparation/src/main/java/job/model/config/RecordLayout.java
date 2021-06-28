package job.model.config;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public class RecordLayout implements Serializable {
    private Map<String, String> fieldTypes;
}
