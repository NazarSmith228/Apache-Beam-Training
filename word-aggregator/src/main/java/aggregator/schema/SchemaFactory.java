package aggregator.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaFactory {

    public static Schema createSchema() {

        return SchemaBuilder
                .record("WordStatistics")
                .namespace("aggregator.model")
                .fields()

                .name("wordsFromAToH")
                .type()
                .map()
                    .values()
                    .array()
                        .items()
                        .stringType()
                .mapDefault(null)

                .name("wordsFromIToQ")
                .type()
                .map()
                    .values()
                    .array()
                        .items()
                    .stringType()
                .mapDefault(null)

                .name("wordsFromRToZ")
                .type()
                .map()
                    .values()
                    .array()
                        .items()
                        .stringType()
                .mapDefault(null)

                .endRecord();
    }
}
