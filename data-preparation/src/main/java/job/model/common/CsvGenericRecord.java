package job.model.common;

import job.model.config.RecordLayout;
import lombok.*;

import java.io.Serializable;
import java.util.Map;

@Data
@Builder
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class CsvGenericRecord implements Serializable {

    private String recordType;
    private RecordLayout recordLayout;
    private Map<String, String> fields;

}
