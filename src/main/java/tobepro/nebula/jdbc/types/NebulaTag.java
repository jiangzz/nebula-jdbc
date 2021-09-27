package tobepro.nebula.jdbc.types;

import java.util.Map;

public class NebulaTag {
    private String name;
    private Map<String, Object> properties;

    public NebulaTag() {
    }

    public NebulaTag(String name, Map<String, Object> properties) {
        this.name = name;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
