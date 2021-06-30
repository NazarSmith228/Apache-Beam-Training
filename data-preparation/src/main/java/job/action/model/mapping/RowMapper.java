package job.action.model.mapping;

import job.action.model.mapping.factory.RowFactory;
import job.action.model.mapping.factory.SchemaFactory;
import job.config.utils.ConfigHandler;
import job.config.utils.FilteringUtils;
import job.model.common.CsvGenericRecord;
import job.model.config.ConfigActions;
import job.model.config.RecordLayout;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowMapper {

    private static final Map<String, RecordLayout> configLayouts;
    private static final Map<String, ConfigActions.MapToAvro> mappingActions;

    static {
        configLayouts = ConfigHandler.getConfigLayouts();
        mappingActions = ConfigHandler.getConfigActions().getMappingActions();
    }

    public static Row mapToRow(CsvGenericRecord genericRecord) {
        String targetRecordType = genericRecord.getRecordType();
        ConfigActions.MapToAvro mappingAction = mappingActions.get(targetRecordType);

        Map<String, String> fieldsMapping = mappingAction.getFieldsMapping();

        List<String> innerRecordTypes = FilteringUtils.extractRecordTypes(fieldsMapping);

        List<Object> innerRows = new ArrayList<>();
        Schema.Builder rowSchemaBuilder = Schema.builder();

        for (String innerRecordType : innerRecordTypes) {
            Map<String, String> innerRecordMapping = FilteringUtils.filterFieldsMapping(fieldsMapping, innerRecordType);

            Map<String, String> innerRecordFieldValues = FilteringUtils.filterRecordMap(
                    genericRecord.getFields(), innerRecordMapping);

            Map<String, String> innerRecordFieldTypes = FilteringUtils.filterRecordMap(
                    configLayouts.get(innerRecordType.toUpperCase()).getFieldTypes(),
                    innerRecordMapping);

            Schema innerSchema = SchemaFactory.createSchema(innerRecordFieldTypes);
            Row innerRow = RowFactory.createRow(innerSchema, innerRecordFieldValues);

            innerRows.add(innerRow);
            rowSchemaBuilder.addRowField(StringUtils.capitalize(innerRecordType), innerSchema);
        }

        Schema resultSchema = rowSchemaBuilder.build();

        return Row.withSchema(resultSchema).addValues(innerRows).build();
    }

}
