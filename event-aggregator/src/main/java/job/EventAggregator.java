package job;

import job.config.JobOptions;
import job.function.ptransform.ParseEventsPTransform;
import job.function.dofn.EventDividerFn;
import job.function.dofn.GroupBySubjectFn;
import job.function.dofn.StatisticsAggregatorFn;
import job.function.dofn.StatisticsTransformerFn;
import job.model.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;
import java.util.Map;

public class EventAggregator {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(JobOptions.class);

        JobOptions jobOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withoutStrictParsing()
                .withValidation()
                .as(JobOptions.class);

        PipelineResult.State state = runProcessingPipeline(jobOptions);

        if (state == PipelineResult.State.DONE) {
            System.out.println("Job succeeded.");
        } else {
            System.err.println("Job did not manage to succeed.");
        }
    }

    private static PipelineResult.State runProcessingPipeline(JobOptions jobOptions) {
        Pipeline pipeline = Pipeline.create(jobOptions);

        PCollection<String> lines = pipeline
                .apply("Read data from JSON",
                        TextIO.read()
                                .from(jobOptions.getInput())
                );

        PCollection<Event> events = lines.apply(new ParseEventsPTransform());

        if (jobOptions.getDebugMode()) {
            events.apply("Write init data to AVRO",
                    AvroIO.write(Event.class)
                            .to(jobOptions.getDebugPath())
                            .withoutSharding()
                            .withSchema(Event.getClassSchema())
                            .withSuffix("/init_data.avro")
            );
        }

        PCollection<KV<String, Iterable<Event>>> eventsGroupedByCity = events
                .apply("Create city - event pairs",
                        MapElements.into(
                                TypeDescriptors.kvs(
                                        TypeDescriptors.strings(), TypeDescriptor.of(Event.class)
                                )
                        ).via(
                                event -> KV.of(event.getCity(), event)
                        )
                )
                .apply("Group events by city",
                        GroupByKey.create()
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                IterableCoder.of(
                                        AvroCoder.of(Event.class, Event.getClassSchema())
                                )
                        )
                );

        PCollection<KV<String, List<KV<Subject, EventInfo>>>> dividedEvents = eventsGroupedByCity
                .apply("Divide Event record into Subject and Info records",
                        ParDo.of(
                                new EventDividerFn()
                        )
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                ListCoder.of(
                                        KvCoder.of(
                                                AvroCoder.of(Subject.class, Subject.getClassSchema()),
                                                AvroCoder.of(EventInfo.class, EventInfo.getClassSchema())
                                        )
                                )
                        )
                );

        PCollection<KV<String, Map<Subject, List<EventInfo>>>> eventsGroupedBySubject = dividedEvents
                .apply("Group EventInfo by Subject",
                        ParDo.of(
                                new GroupBySubjectFn()
                        )
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                MapCoder.of(
                                        AvroCoder.of(Subject.class, Subject.getClassSchema()),
                                        ListCoder.of(
                                                AvroCoder.of(EventInfo.class, EventInfo.getClassSchema())
                                        )
                                )
                        )
                );

        PCollection<KV<String, Map<Subject, List<Activity>>>> eventStatistics = eventsGroupedBySubject
                .apply("Perform statistics calculation on the aggregated events",
                        ParDo.of(new StatisticsAggregatorFn())
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                MapCoder.of(
                                        AvroCoder.of(Subject.class, Subject.getClassSchema()),
                                        ListCoder.of(
                                                AvroCoder.of(Activity.class, Activity.getClassSchema())
                                        )
                                )
                        )
                );

        PCollection<KV<String, List<EventStatistics>>> statisticsGroupedByCity = eventStatistics
                .apply("Map to EventStatistics.class in order to write to AVRO",
                        ParDo.of(
                                new StatisticsTransformerFn()
                        )
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                ListCoder.of(
                                        AvroCoder.of(EventStatistics.class, EventStatistics.getClassSchema())
                                )
                        )
                );

        statisticsGroupedByCity
                .apply("Creating summary output",
                        ParDo.of(
                                new DoFn<KV<String, List<EventStatistics>>, Summary>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext ctx) {
                                        KV<String, List<EventStatistics>> element = ctx.element();
                                        Summary summary = Summary.newBuilder()
                                                .setCity(element.getKey())
                                                .setActivities(element.getValue())
                                                .build();

                                        ctx.output(summary);
                                    }
                                }
                        )
                )
                .setCoder(
                        AvroCoder.of(Summary.class, Summary.getClassSchema())
                )
                .apply("Write summary output to AVRO",
                        FileIO.<String, Summary>writeDynamic()
                                .by(Summary::getCity)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(AvroIO.sink(Summary.class))
                                .withNaming(
                                        (key) -> FileIO.Write.defaultNaming(key, ".avro")
                                )
                                .to(jobOptions.getOutput())
                );

        return pipeline.run().waitUntilFinish();
    }

}