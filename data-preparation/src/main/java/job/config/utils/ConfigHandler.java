package job.config.utils;

import job.model.config.ConfigActions;
import job.model.config.RecordLayout;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class ConfigHandler {

    private static final String PROFILE = "profile.yaml";
    private static final String LAYOUTS = "layouts";
    private static final String ACTIONS = "actions";

    private static final Yaml yamlParser = new Yaml();

    private static Map<String, RecordLayout> configLayouts;
    private static ConfigActions configActions;


    public static void loadConfig(String path) {
        try (InputStream inputStream = new FileInputStream(path)) {
            Map<String, Object> config = yamlParser.load(inputStream);
            parseConfig(config);
        } catch (IOException ex) {
            throw new MissingResourceException("Failed to load configuration profile",
                    FileInputStream.class.getName(), PROFILE);
        }
    }

    private static void parseConfig(Map<String, Object> config) {
        Map<String, Map<String, String>> layouts = (Map<String, Map<String, String>>) config.get(LAYOUTS);
        parseLayouts(layouts);

        List<Map<String, Object>> actions = (List<Map<String, Object>>) config.get(ACTIONS);
        parseActions(actions);
    }

    private static void parseLayouts(Map<String, Map<String, String>> layouts) {
        Map<String, RecordLayout> layoutMap = new HashMap<>();
        layouts.forEach((key, value) -> layoutMap.put(key, new RecordLayout(value)));
        configLayouts = layoutMap;
    }

    private static void parseActions(List<Map<String, Object>> actions) {
        List<ConfigActions.Validate> validationActions = parseValidationActions(actions);
        List<ConfigActions.GroupBy> groupingActions = parseGroupingActions(actions);
        Map<String, ConfigActions.MapToAvro> mappingActions = parseMappingActions(actions);

        configActions = ConfigActions.builder()
                .validateActions(validationActions)
                .groupingActions(groupingActions)
                .mappingActions(mappingActions)
                .build();
    }

    private static List<ConfigActions.Validate> parseValidationActions(List<Map<String, Object>> actions) {
        return actions.stream()
                .filter(entry -> entry.get("type").equals("validate"))
                .map(
                        entry -> ConfigActions.Validate.builder()
                                .recordType((String) entry.get("recordType"))
                                .fieldName((String) entry.get("field"))
                                .validationConstraint((String) entry.get("constraint"))
                                .build()
                )
                .collect(Collectors.toList());
    }

    private static List<ConfigActions.GroupBy> parseGroupingActions(List<Map<String, Object>> actions) {
        return actions.stream()
                .filter(entry -> entry.get("type").equals("groupBy"))
                .map(
                        entry -> ConfigActions.GroupBy.builder()
                                .recordTypes((List<String>) entry.get("recordTypes"))
                                .groupingKey((String) entry.get("groupingKey"))
                                .resultRecordName((String) entry.get("resultRecordName"))
                                .build()
                ).collect(Collectors.toList());
    }

    private static Map<String, ConfigActions.MapToAvro> parseMappingActions(List<Map<String, Object>> actions) {
        Map<String, ConfigActions.MapToAvro> mappingActions = new HashMap<>();
        actions.stream()
                .filter(entry -> entry.get("type").equals("mapToAvro"))
                .forEach(
                        entry -> {
                            ConfigActions.MapToAvro action = ConfigActions.MapToAvro.builder()
                                    .targetSchema((String) entry.get("targetSchema"))
                                    .fieldsMapping((Map<String, String>) entry.get("mapping"))
                                    .build();
                            String recordType = (String) entry.get("sourceLayout");
                            mappingActions.put(recordType, action);
                        }
                );
        return mappingActions;
    }

    public static Map<String, RecordLayout> getConfigLayouts() {
        return configLayouts;
    }

    public static ConfigActions getConfigActions() {
        return configActions;
    }

}
