package job.action.model.mapping;

import job.action.model.mapping.factory.RowFactory;
import job.action.model.mapping.factory.SchemaFactory;
import job.config.utils.ConfigHandler;
import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import job.model.config.RecordLayout;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RowMapper {

    private static final Map<String, RecordLayout> configLayouts;
    private static final Map<String, ConfigActions.MapToAvro> mappingActions;

    static {
        configLayouts = ConfigHandler.getConfigLayouts();
        mappingActions = ConfigHandler.getConfigActions().getMappingActions();
    }

    public static KV<String, Row> mapToRow(CsvGenericRecord genericRecord) {
        String targetRecordType = genericRecord.getRecordType();
        ConfigActions.MapToAvro mappingAction = mappingActions.get(targetRecordType);

        Map<String, String> fieldsMapping = mappingAction.getFieldsMapping();

        List<String> innerRecordTypes = fieldsMapping.values()
                .stream()
                //vulnerability
                .map(val -> val.split("\\.")[0])
                .distinct()
                .collect(Collectors.toList());

        List<Object> innerRows = new ArrayList<>();
        Schema.Builder rowSchemaBuilder = Schema.builder();

        for (String innerRecordType : innerRecordTypes) {
            Map<String, String> innerRecordMapping = fieldsMapping.entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().contains(innerRecordType))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Map<String, String> innerRecordFieldValues = filterRecordMap(genericRecord.getFields(), innerRecordMapping);

            Map<String, String> innerRecordFieldTypes = filterRecordMap(
                    configLayouts.get(innerRecordType.toUpperCase()).getFieldTypes(),
                    innerRecordMapping);

            Schema innerSchema = SchemaFactory.createInnerSchema(innerRecordFieldTypes);
            Row innerRow = RowFactory.createInnerRow(innerSchema, innerRecordFieldValues);

            innerRows.add(innerRow);
            rowSchemaBuilder.addRowField(StringUtils.capitalize(innerRecordType), innerSchema);
        }

        Schema resultSchema = rowSchemaBuilder.build();
        Row resultRow = Row.withSchema(resultSchema).addValues(innerRows).build();
        //vulnerability
        String targetSchema = mappingAction.getTargetSchema().split("\\.")[0];

        return KV.of(targetSchema, resultRow);
    }

    private static Map<String, String> filterRecordMap(Map<String, String> recordMap, Map<String, String> innerRecordMapping) {
        return recordMap.entrySet()
                .stream()
                .filter(entry -> innerRecordMapping.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
