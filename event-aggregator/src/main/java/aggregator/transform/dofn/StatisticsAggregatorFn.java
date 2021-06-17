package aggregator.transform.dofn;

import aggregator.model.Activity;
import aggregator.model.EventInfo;
import aggregator.model.Subject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Days;
import org.joda.time.LocalDateTime;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatisticsAggregatorFn extends DoFn<KV<String, Map<Subject, List<EventInfo>>>, KV<String, Map<Subject, List<Activity>>>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
        Map<Subject, List<EventInfo>> eventsGroupedBySubject = ctx.element().getValue();
        Map<Subject, List<Activity>> newEventMap = new HashMap<>();

        eventsGroupedBySubject.forEach(
                (subject, eventInfos) ->
                {
                    Map<String, List<EventInfo>> eventsGroupedByType = eventInfos.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            EventInfo::getEventType
                                    )
                            );

                    List<Activity> activities = new ArrayList<>();

                    eventsGroupedByType.forEach(
                            (eventType, particularTypeEvents) ->
                            {
                                long past7DaysCount = pastTimeRecordCount(particularTypeEvents, 7, false);
                                long past7DaysUniqueCount = pastTimeRecordCount(particularTypeEvents, 7, true);
                                long past30DaysCount = pastTimeRecordCount(particularTypeEvents, 30, false);
                                long past30DaysUniqueCount = pastTimeRecordCount(particularTypeEvents, 30, true);

                                Activity activity = Activity.newBuilder()
                                        .setEventType(eventType)
                                        .setPastWeekCount(past7DaysCount)
                                        .setPastWeekUniqueCount(past7DaysUniqueCount)
                                        .setPastMonthCount(past30DaysCount)
                                        .setPastMonthUniqueCount(past30DaysUniqueCount)
                                        .build();

                                activities.add(activity);

                            }
                    );

                    newEventMap.put(subject, activities);
                }
        );

        ctx.output(KV.of(ctx.element().getKey(), newEventMap));
    }

    private long pastTimeRecordCount(List<EventInfo> eventInfos, int days, boolean unique) {
        Stream<EventInfo> filtered = eventInfos.stream()
                .filter(
                        eventInfo -> {
                            LocalDateTime eventTime = new LocalDateTime(eventInfo.getTimestamp());
                            LocalDateTime now = LocalDateTime.now();
                            return Days.daysBetween(eventTime, now).getDays() <= days;
                        }
                );

        if (unique) {
            filtered = filtered.filter(filterByKey(EventInfo::getUserId));
        }

        return filtered.count();
    }

    private <T> Predicate<T> filterByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> set = ConcurrentHashMap.newKeySet();
        return t -> set.add(keyExtractor.apply(t));
    }
}
