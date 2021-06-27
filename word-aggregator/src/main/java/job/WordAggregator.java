package job;

import job.config.WordOptions;
import job.function.GroupByFirstLetterPTransform;
import job.function.MapCombinerPTransform;
import job.function.PCollectionConverterPTransform;
import job.model.WordStatistics;
import job.schema.SchemaFactory;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WordAggregator {

    public static void main(String[] args) {

        WordOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .withoutStrictParsing()
                .as(WordOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        Schema schema = SchemaFactory.createSchema();

        pipeline
                .apply("Create pipeline with schema",
                        Create.of(schema.toString(true))
                )
                .apply("Write schema to file",
                        TextIO.write()
                                .to(options.getSchemaPath())
                                .withSuffix(".avsc")
                                .withoutSharding()
                );

        PCollection<String> readLines = pipeline
                .apply("Read data from file(s)",
                        TextIO.read()
                                .from(options.getInput())
                );

        PCollection<String> nonEmptyLines = readLines.apply("Filter non-empty lines",
                Filter.by(
                        line -> !line.isEmpty()
                )
        );

        if (options.getDebugMode()) {
            writeMediateData(nonEmptyLines, options.getDebugPath(), "/non-empty.txt");
        }

        PCollection<String> separateWords = nonEmptyLines
                .apply("Divide data into separate words",
                        FlatMapElements.into(
                                TypeDescriptors.strings()
                        )
                                .via(
                                        line -> Arrays.asList(line.split("[^\\p{L}]+"))
                                )
                );

        if (options.getDebugMode()) {
            writeMediateData(separateWords, options.getDebugPath(), "/separate_words.txt");
        }


        PCollection<String> AToHWords = extractByPattern(separateWords, "(?i)^[a-h].*");
        PCollection<String> IToQWords = extractByPattern(separateWords, "(?i)^[i-q].*");
        PCollection<String> RToZWords = extractByPattern(separateWords, "(?i)^[r-z].*");

        PCollectionList<String> allWords = PCollectionList.of(AToHWords).and(IToQWords).and(RToZWords);

        PCollectionList<KV<String, Iterable<String>>> allWordsGrouped = allWords
                .apply("Group all words in each PCollection by first letter",
                        new GroupByFirstLetterPTransform()
                );

        PCollectionList<Map<String, List<String>>> allMaps = allWordsGrouped
                .apply("Convert all PCollection<KV> to PCollection<Map>",
                        new PCollectionConverterPTransform()
                );

        PCollectionList<Map<String, List<String>>> combinedMaps = allMaps
                .apply("Combine all Maps per single PCollection into a single Map",
                        new MapCombinerPTransform()
                );

        PCollection<Map<String, List<String>>> wordsMapsAToH = combinedMaps.get(0);
        PCollection<Map<String, List<String>>> wordsMapsIToQ = combinedMaps.get(1);
        PCollection<Map<String, List<String>>> wordsMapsRToZ = combinedMaps.get(2);

        PCollection<WordStatistics> AToHStatistics = wordsMapsAToH
                .apply("Get statistics for words starting with A-H",
                        ParDo.of(
                                new DoFn<Map<String, List<String>>, WordStatistics>() {
                                    @ProcessElement
                                    public void process(ProcessContext ctx) {
                                        Map<String, List<String>> element = ctx.element();
                                        WordStatistics wordStatistics = WordStatistics.newBuilder()
                                                .setWordsFromAToH(element)
                                                .build();

                                        ctx.output(wordStatistics);
                                    }
                                }
                        )
                )
                .setCoder(
                        AvroCoder.of(WordStatistics.class, WordStatistics.getClassSchema())
                );

        PCollection<WordStatistics> IToQStatistics = wordsMapsIToQ
                .apply("Get statistics for words starting with I-Q",
                        ParDo.of(
                                new DoFn<Map<String, List<String>>, WordStatistics>() {
                                    @ProcessElement
                                    public void process(ProcessContext ctx) {
                                        Map<String, List<String>> element = ctx.element();
                                        WordStatistics wordStatistics = WordStatistics.newBuilder()
                                                .setWordsFromIToQ(element)
                                                .build();

                                        ctx.output(wordStatistics);
                                    }
                                }
                        )
                )
                .setCoder(
                        AvroCoder.of(WordStatistics.class, WordStatistics.getClassSchema())
                );

        PCollection<WordStatistics> RToZStatistics = wordsMapsRToZ
                .apply("Get statistics for words starting with R-Z",
                        ParDo.of(
                                new DoFn<Map<String, List<String>>, WordStatistics>() {
                                    @ProcessElement
                                    public void process(ProcessContext ctx) {
                                        Map<String, List<String>> element = ctx.element();
                                        WordStatistics wordStatistics = WordStatistics.newBuilder()
                                                .setWordsFromRToZ(element)
                                                .build();

                                        ctx.output(wordStatistics);
                                    }
                                }
                        )
                )
                .setCoder(
                        AvroCoder.of(WordStatistics.class, WordStatistics.getClassSchema())
                );

        PCollectionList<WordStatistics> wordStatisticsPCollectionList = PCollectionList.of(AToHStatistics).and(IToQStatistics).and(RToZStatistics);

        PCollection<WordStatistics> wordStatisticsPCollection = wordStatisticsPCollectionList
                .apply("Merge all PCollection<WordStatistics>",
                        Flatten.pCollections()
                )
                .setCoder(
                        AvroCoder.of(WordStatistics.class, WordStatistics.getClassSchema())
                );

        PCollection<WordStatistics> combinedStatistics = wordStatisticsPCollection
                .apply("Combine all WordStatistics objects into one",
                        Combine.globally(
                                (SerializableFunction<Iterable<WordStatistics>, WordStatistics>) input -> {
                                    WordStatistics combined = new WordStatistics();
                                    input.forEach(
                                            statistics -> {
                                                Map<String, List<String>> wordsFromAToH = statistics.getWordsFromAToH();
                                                if (wordsFromAToH != null) {
                                                    combined.setWordsFromAToH(wordsFromAToH);
                                                }

                                                Map<String, List<String>> wordsFromIToQ = statistics.getWordsFromIToQ();
                                                if (wordsFromIToQ != null) {
                                                    combined.setWordsFromIToQ(wordsFromIToQ);
                                                }

                                                Map<String, List<String>> wordsFromRToZ = statistics.getWordsFromRToZ();
                                                if (wordsFromRToZ != null) {
                                                    combined.setWordsFromRToZ(wordsFromRToZ);
                                                }
                                            }

                                    );
                                    return combined;
                                }
                        )
                )
                .setCoder(
                        AvroCoder.of(WordStatistics.class, WordStatistics.getClassSchema())
                );

        combinedStatistics
                .apply("Write the statistics to AVRO",
                        AvroIO.write(WordStatistics.class)
                                .to(options.getOutput())
                                .withoutSharding()
                                .withSuffix(".avro")
                );

        pipeline.run().waitUntilFinish();
    }

    private static void writeMediateData(PCollection<String> pCollection, String path, String suffix) {
        pCollection
                .apply("Write elements of PCollection to file",
                        TextIO.write()
                                .to(path)
                                .withoutSharding()
                                .withSuffix(suffix)
                );
    }

    private static PCollection<String> extractByPattern(PCollection<String> pCollection, String pattern) {
        return pCollection
                .apply("Extract words matching passed patter",
                        Regex.matches(pattern)
                );
    }
}
