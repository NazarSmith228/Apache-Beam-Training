package aggregator.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapCombinerPTransform extends PTransform<PCollectionList<Map<String, List<String>>>, PCollectionList<Map<String, List<String>>>> {
    @Override
    public PCollectionList<Map<String, List<String>>> expand(PCollectionList<Map<String, List<String>>> input) {
        List<PCollection<Map<String, List<String>>>> output = input.getAll()
                .stream()
                .map(
                        mapPCollection -> mapPCollection.apply(
                                Combine.globally(
                                        (SerializableFunction<Iterable<Map<String, List<String>>>, Map<String, List<String>>>) mapIterable -> {
                                            Map<String, List<String>> aggregationMap = new HashMap<>();
                                            mapIterable.forEach(
                                                    (Map<String, List<String>> map) ->
                                                            map.forEach(
                                                                    aggregationMap::put
                                                            )
                                            );
                                            return aggregationMap;
                                        }
                                )
                        )
                )
                .collect(Collectors.toList());

        return PCollectionList.of(output);
    }
}
