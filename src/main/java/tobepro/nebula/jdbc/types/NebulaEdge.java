package tobepro.nebula.jdbc.types;

import java.util.Map;

public class NebulaEdge {
    private Object srcVid;
    private Object dstVid;
    private String name;
    private Long ranking;
    private Map<String, Object> properties;

    public Object getSrcVid() {
        return srcVid;
    }

    public void setSrcVid(Object srcVid) {
        this.srcVid = srcVid;
    }

    public Object getDstVid() {
        return dstVid;
    }

    public void setDstVid(Object dstVid) {
        this.dstVid = dstVid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getRanking() {
        return ranking;
    }

    public void setRanking(Long ranking) {
        this.ranking = ranking;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
