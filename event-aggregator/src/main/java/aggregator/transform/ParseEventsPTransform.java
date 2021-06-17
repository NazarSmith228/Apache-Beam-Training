package aggregator.transform;

import aggregator.model.Event;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class ParseEventsPTransform extends PTransform<PCollection<String>, PCollection<Event>> {
    @Override
    public PCollection<Event> expand(PCollection<String> input) {
        return input.apply("Parse strings to Event.class",
                ParseJsons.of(Event.class)
        )
                .setCoder(
                        AvroCoder.of(Event.class, Event.getClassSchema())
                );
    }
}
