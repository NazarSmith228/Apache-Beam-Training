package sample.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
public class EventSubject implements Serializable {
    private String type;
    private Integer id;
}
