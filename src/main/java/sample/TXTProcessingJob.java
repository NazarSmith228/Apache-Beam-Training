package sample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import sample.model.Event;
import sample.model.EventSubject;
import sample.model.SimpleEvent;
import sample.options.SampleOptions;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Sample processing job which aggregates event data and writes output data to .txt file
 * <p>
 * P.S not sure why i created it, but let it be...
 */
public class TXTProcessingJob {

    public static void main(String[] args) {

        PipelineOptionsFactory.register(SampleOptions.class);

        SampleOptions sampleOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(SampleOptions.class);

        Pipeline p = Pipeline.create(sampleOptions);

        PCollection<String> lines = p.apply("Read data from JSON",
                TextIO.read().from(sampleOptions.getInput())
        );

        PCollection<Event> events = lines
                .apply("Convert JSON rows to Event.class",
                        ParseJsons.of(Event.class)
                )
                .setCoder(
                        SerializableCoder.of(Event.class)
                );

        PCollection<KV<String, Event>> cityEventKVs = events
                .apply("Create city - event pairs",
                        GroupByKey.compose(
                                (SerializableFunction<PCollection<Event>, PCollection<KV<String, Event>>>) input ->
                                        input.apply("Use city of the event as a key",
                                                WithKeys.of(
                                                        (SerializableFunction<Event, String>) Event::getCity
                                                )
                                        )
                        )
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(), SerializableCoder.of(Event.class)
                        )
                );

        PCollection<KV<String, Iterable<Event>>> eventsByCity = cityEventKVs
                .apply("Group events by city",
                        GroupByKey.create()
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                IterableCoder.of(
                                        SerializableCoder.of(Event.class)
                                )
                        )
                );

        PCollection<KV<String, List<KV<EventSubject, SimpleEvent>>>> dividedEventsByCity = eventsByCity
                .apply("Divide Event.class into EventSubject.class and SimpleEvent.class",
                        ParDo.of(new IterableEventTransformerFn())
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                ListCoder.of(
                                        KvCoder.of(
                                                SerializableCoder.of(EventSubject.class), SerializableCoder.of(SimpleEvent.class)
                                        )
                                )
                        )
                );

        PCollection<KV<String, Map<EventSubject, List<SimpleEvent>>>> eventsGroupedByCityAndSubject = dividedEventsByCity
                .apply("Group simple events by subject",
                        ParDo.of(new SimpleEventCombinerFn())
                )
                .setCoder(
                        KvCoder.of(
                                SerializableCoder.of(String.class),
                                MapCoder.of(
                                        SerializableCoder.of(EventSubject.class),
                                        ListCoder.of(
                                                SerializableCoder.of(SimpleEvent.class)
                                        )
                                )
                        )
                );

        PCollection<KV<String, String>> stringsData = eventsGroupedByCityAndSubject
                .apply("Map finally grouped events into strings",
                        ParDo.of(new EventMapToStringConverterFn())
                );

        stringsData
                .apply("Map city - events pairs to single string",
                        MapElements.into(
                                TypeDescriptors.strings()
                        )
                                .via(
                                        (KV<String, String> kv) ->
                                                String.format("City: %s\n%s\n", kv.getKey(), kv.getValue())
                                )
                )
                .apply("Write data to file",
                        TextIO.write()
                                .to(sampleOptions.getOutput())
                                .withoutSharding()
                                .withSuffix(".txt")
                );

        p.run().waitUntilFinish();
    }

    static class IterableEventTransformerFn extends DoFn<KV<String, Iterable<Event>>, KV<String, List<KV<EventSubject, SimpleEvent>>>> {
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Event>> input, OutputReceiver<KV<String, List<KV<EventSubject, SimpleEvent>>>> receiver) {
            List<KV<EventSubject, SimpleEvent>> dividedEvents = StreamSupport
                    .stream(Objects.requireNonNull(input.getValue()).spliterator(), false)
                    .map(
                            event -> KV.of(event.getEventSubject(), new SimpleEvent(event))
                    )
                    .collect(Collectors.toList());

            receiver.output(
                    KV.of(input.getKey(), dividedEvents)
            );
        }
    }

    static class SimpleEventCombinerFn extends DoFn<KV<String, List<KV<EventSubject, SimpleEvent>>>, KV<String, Map<EventSubject, List<SimpleEvent>>>> {
        @ProcessElement
        public void processElement(@Element KV<String, List<KV<EventSubject, SimpleEvent>>> element,
                                   OutputReceiver<KV<String, Map<EventSubject, List<SimpleEvent>>>> receiver) {
            List<KV<EventSubject, SimpleEvent>> eventList = element.getValue();
            Map<EventSubject, List<SimpleEvent>> groupedEvents = Objects.requireNonNull(eventList)
                    .stream()
                    .collect(
                            Collectors.groupingBy(
                                    KV::getKey,
                                    Collectors.mapping(
                                            KV::getValue, Collectors.toList()
                                    )
                            )
                    );

            receiver.output(KV.of(element.getKey(), groupedEvents));
        }
    }

    static class EventMapToStringConverterFn extends DoFn<KV<String, Map<EventSubject, List<SimpleEvent>>>, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element KV<String, Map<EventSubject, List<SimpleEvent>>> element, OutputReceiver<KV<String, String>> receiver) {
            Map<EventSubject, List<SimpleEvent>> groupedEvents = element.getValue();
            Set<Map.Entry<EventSubject, List<SimpleEvent>>> entrySet = Objects.requireNonNull(groupedEvents).entrySet();

            int count = 1;
            StringBuilder sb = new StringBuilder();
            sb.append("\tSubjects:\n").append("\t\t[\n");

            for (Map.Entry<EventSubject, List<SimpleEvent>> entry : entrySet) {
                EventSubject subject = entry.getKey();
                List<SimpleEvent> simpleEvents = entry.getValue();

                sb.append(String.format("\t\t\tSubject#%d - %s\n", count++, subject));
                for (SimpleEvent simpleEvent : simpleEvents) {
                    sb.append(String.format("\t\t\t\tEvent: %s\n", simpleEvent));
                }

            }
            sb.append("\t\t]");
            receiver.output(KV.of(element.getKey(), sb.toString()));
        }
    }
}