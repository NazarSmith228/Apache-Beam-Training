package job.other.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.util.Date;

@NoArgsConstructor
@Data
@DefaultCoder(SerializableCoder.class)
public class Event implements Serializable {

    private Integer id;
    private String userId;
    private String city;
    private String eventType;
    private Date timestamp;
    private EventSubject eventSubject;

}
