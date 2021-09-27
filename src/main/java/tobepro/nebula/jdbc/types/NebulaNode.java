package tobepro.nebula.jdbc.types;

import java.util.List;

public class NebulaNode {
    private Object vid;
    private List<NebulaTag> tags;

    public Object getVid() {
        return vid;
    }

    public void setVid(Object vid) {
        this.vid = vid;
    }

    public List<NebulaTag> getTags() {
        return tags;
    }

    public void setTags(List<NebulaTag> tags) {
        this.tags = tags;
    }

    public NebulaNode(Object vid, List<NebulaTag> tags) {
        this.vid = vid;
        this.tags = tags;
    }

    public NebulaNode() {
    }
}
