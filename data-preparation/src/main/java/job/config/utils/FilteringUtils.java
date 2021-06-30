package job.config.utils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FilteringUtils {

    public static List<String> extractRecordTypes(Map<String, String> fieldsMapping) {
        return fieldsMapping.values()
                .stream()
                //vulnerability
                .map(val -> val.split("\\.")[0])
                .distinct()
                .collect(Collectors.toList());
    }

    public static Map<String, String> filterFieldsMapping(Map<String, String> fieldsMapping, String recordType) {
        return fieldsMapping.entrySet()
                .stream()
                .filter(entry -> entry.getValue().contains(recordType))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, String> filterRecordMap(Map<String, String> recordMap, Map<String, String> recordMapping) {
        return recordMap.entrySet()
                .stream()
                .filter(entry -> recordMapping.containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
