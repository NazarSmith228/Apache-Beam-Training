package job.function;

import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.List;
import java.util.stream.Collectors;

public class GroupByFirstLetterPTransform extends PTransform<PCollectionList<String>, PCollectionList<KV<String, Iterable<String>>>> {
    @Override
    public PCollectionList<KV<String, Iterable<String>>> expand(PCollectionList<String> input) {
        List<PCollection<KV<String, Iterable<String>>>> pCollectionList = input.getAll()
                .stream()
                .map(this::groupWordsByFirstLetter)
                .collect(Collectors.toList());

        return PCollectionList.of(pCollectionList);
    }


    private PCollection<KV<String, Iterable<String>>> groupWordsByFirstLetter(PCollection<String> words) {
        return words
                .apply("Assign each word to a key representing its first letter",
                        WithKeys.of(
                                word -> String.valueOf(word.charAt(0)).toLowerCase()
                        )
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(), StringUtf8Coder.of()
                        )
                )
                .apply("Group words by their first letter",
                        GroupByKey.create()
                )
                .setCoder(
                        KvCoder.of(
                                StringUtf8Coder.of(),
                                IterableCoder.of(
                                        StringUtf8Coder.of()
                                )
                        )
                );
    }
}
