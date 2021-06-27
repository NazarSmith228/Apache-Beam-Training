package job.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class PCollectionConverterPTransform extends PTransform<PCollectionList<KV<String, Iterable<String>>>, PCollectionList<Map<String, List<String>>>> {
    @Override
    public PCollectionList<Map<String, List<String>>> expand(PCollectionList<KV<String, Iterable<String>>> input) {
        List<PCollection<Map<String, List<String>>>> mapPCollections = input.getAll()
                .stream()
                .map(
                        pCollection -> pCollection.apply(
                                ParDo.of(
                                        new PairsToMapConverterFn()
                                )
                        )
                )
                .collect(Collectors.toList());

        return PCollectionList.of(mapPCollections);
    }

    private static class PairsToMapConverterFn extends DoFn<KV<String, Iterable<String>>, Map<String, List<String>>> {
        @ProcessElement
        public void process(@Element KV<String, Iterable<String>> element, OutputReceiver<Map<String, List<String>>> receiver) {
            Iterable<String> stringIterable = element.getValue();
            List<String> stringList = StreamSupport
                    .stream(stringIterable.spliterator(), false)
                    .collect(Collectors.toList());

            Map<String, List<String>> kvMap = new HashMap<>();
            kvMap.put(element.getKey(), stringList);

            receiver.output(kvMap);
        }
    }
}
