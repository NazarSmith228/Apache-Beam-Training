package dataflow.transform.dofn;

import dataflow.model.Activity;
import dataflow.model.EventStatistics;
import dataflow.model.Subject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatisticsTransformerFn extends DoFn<KV<String, Map<Subject, List<Activity>>>, KV<String, List<EventStatistics>>> {
    @ProcessElement
    public void processElement(@Element KV<String, Map<Subject, List<Activity>>> element, OutputReceiver<KV<String, List<EventStatistics>>> receiver) {
        Map<Subject, List<Activity>> value = element.getValue();
        List<EventStatistics> statistics = value.entrySet()
                .stream()
                .map(
                        entry -> EventStatistics.newBuilder()
                                .setEventSubject(entry.getKey())
                                .setActivities(entry.getValue())
                                .build()
                )
                .collect(Collectors.toList());

        KV<String, List<EventStatistics>> newKV = KV.of(element.getKey(), statistics);
        receiver.output(newKV);
    }
}
