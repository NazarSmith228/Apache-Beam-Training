package aggregator.transform.dofn;

import aggregator.model.Event;
import aggregator.model.EventInfo;
import aggregator.model.Subject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class EventDividerFn extends DoFn<KV<String, Iterable<Event>>, KV<String, List<KV<Subject, EventInfo>>>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<Event>> element, OutputReceiver<KV<String, List<KV<Subject, EventInfo>>>> receiver) {
        Iterable<Event> events = element.getValue();

        List<KV<Subject, EventInfo>> dividedEvents = StreamSupport.stream(events.spliterator(), false)
                .map(
                        event -> {
                            EventInfo info = EventInfo.newBuilder()
                                    .setId(event.getId())
                                    .setUserId(event.getUserId())
                                    .setTimestamp(event.getTimestamp())
                                    .setEventType(event.getEventType())
                                    .build();
                            return KV.of(event.getEventSubject(), info);
                        }
                )
                .collect(Collectors.toList());

        KV<String, List<KV<Subject, EventInfo>>> outputKV = KV.of(element.getKey(), dividedEvents);
        receiver.output(outputKV);
    }
}
