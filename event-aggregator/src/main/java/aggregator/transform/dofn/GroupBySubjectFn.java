package aggregator.transform.dofn;

import aggregator.model.EventInfo;
import aggregator.model.Subject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GroupBySubjectFn extends DoFn<KV<String, List<KV<Subject, EventInfo>>>, KV<String, Map<Subject, List<EventInfo>>>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        KV<String, List<KV<Subject, EventInfo>>> element = ctx.element();
        List<KV<Subject, EventInfo>> dividedEvents = element.getValue();

        Map<Subject, List<EventInfo>> groupedBySubject = dividedEvents.stream()
                .collect(
                        Collectors.groupingBy(
                                KV::getKey,
                                Collectors.mapping(
                                        KV::getValue, Collectors.toList()
                                )
                        )
                );

        KV<String, Map<Subject, List<EventInfo>>> outputKv = KV.of(element.getKey(), groupedBySubject);
        ctx.output(outputKv);
    }
}
