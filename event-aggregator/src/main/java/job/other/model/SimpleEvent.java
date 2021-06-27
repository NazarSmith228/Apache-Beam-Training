package job.other.model;

import lombok.*;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;
import java.util.Date;

@NoArgsConstructor
@Data
@DefaultCoder(SerializableCoder.class)
public class SimpleEvent implements Serializable {

    private Integer id;
    private String userId;
    private String city;
    private Date timestamp;
    private String eventType;

    public SimpleEvent(Event event) {
        this.id = event.getId();
        this.userId = event.getUserId();
        this.city = event.getCity();
        this.timestamp = event.getTimestamp();
        this.eventType = event.getEventType();
    }
}
