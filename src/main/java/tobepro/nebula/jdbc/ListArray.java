package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.types.NebulaNode;
import tobepro.nebula.jdbc.utils.NebulaUtil;
import com.vesoft.nebula.client.graph.data.ValueWrapper;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ListArray extends NebulaArray {
    public static final String TYPE_NOT_SUPPORTED = "Type {0} not supported";
    private final List<?> list;
    private final int type;

    public ListArray(List<?> list, int type) {
        this.list = list;
        this.type = type;
    }
    
    public ListArray(String typeName, Object[] elements) throws SQLException{
        this.list = Arrays.asList(elements);
        int type;
        switch (typeName) {
            case "VARCHAR":
                type = Types.VARCHAR;
                break;
            case "INTEGER":
                type = Types.INTEGER;
                break;
            case "BOOLEAN":
                type = Types.BOOLEAN;
                break;
            case "DOUBLE":
                type = Types.DOUBLE;
                break;
            case "JAVA_OBJECT":
                type = Types.JAVA_OBJECT;
                break;
            default:
                throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, typeName));
        }
        this.type = type;
    }
    
    public ListArray(List<ValueWrapper> elements) throws SQLException {
        int type;
        List<Object> objectList = new ArrayList<>();
        if (elements.isEmpty()) {
            type = Types.JAVA_OBJECT;
        }
        ValueWrapper valueWrapper = elements.get(0);
        
        if (valueWrapper.isString()) {
            type = Types.VARCHAR;
            for (ValueWrapper element : elements) {
                try {
                    objectList.add(element.asString());
                } catch (UnsupportedEncodingException e) {
                    throw new SQLException(e);
                }
            }
        } else if (valueWrapper.isLong()) {
            type = Types.INTEGER;
            for (ValueWrapper element : elements) {
                objectList.add(element.asLong());
            }
        } else if (valueWrapper.isBoolean()) {
            type = Types.BOOLEAN;
            for (ValueWrapper element : elements) {
                objectList.add(element.asBoolean());
            }
        } else if (valueWrapper.isDouble()) {
            type = Types.DOUBLE;
            for (ValueWrapper element : elements) {
                objectList.add(element.asDouble());
            }
        } else if (valueWrapper.isList()) {
            type = Types.ARRAY;
            for (ValueWrapper element : elements) {
                objectList.add(new ListArray(element.asList()));
            }
        } else if (valueWrapper.isDate() 
                || valueWrapper.isTime() 
                || valueWrapper.isDateTime() 
                || valueWrapper.isVertex() 
                || valueWrapper.isEdge() 
                || valueWrapper.isPath()) {
            type = Types.JAVA_OBJECT;
            
            Class<?> clazz;
            if (valueWrapper.isDate()) {
                clazz = Date.class;
            } else if (valueWrapper.isTime()) {
                clazz = Time.class;
            } else if (valueWrapper.isDateTime()) {
                clazz = Timestamp.class;
            } else if (valueWrapper.isVertex()) {
                // TODO 处理特殊类型
                clazz = NebulaNode.class;
            } else {
                throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, ""));
            }

            for (ValueWrapper element : elements) {
                objectList.add(NebulaUtil.convertToJavaType(element, clazz));
            }
        } else {
            throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, ""));
        }
        
        this.list = objectList;
        this.type = type;
    }

    @Override
    public int getBaseType() throws SQLException {
        if (!SUPPORTED_TYPES.contains(this.type)) {
            throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, this.type));
        }
        
        return this.type;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        if (!SUPPORTED_TYPES.contains(this.type)) {
            throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, this.type));
        }
        
        String name;
        switch (this.type) {
            case Types.VARCHAR:
                name = "VARCHAR";
                break;
            case Types.INTEGER:
                name = "INTEGER";
                break;
            case Types.BOOLEAN:
                name = "BOOLEAN";
                break;
            case Types.JAVA_OBJECT:
                name = "JAVA_OBJECT";
                break;
            default:
                throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, this.type));
        }
        return name;
    }

    @Override
    public Object getArray() throws SQLException {
        if (!SUPPORTED_TYPES.contains(this.type)) {
            throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, this.type));
        }
        
        Object result;
        
        switch (this.type) {
            case Types.VARCHAR:
                result = this.list.stream().map(o -> (String) o).toArray(String[]::new);
                break;
            case Types.INTEGER:
                result = this.list.stream().map(o -> (Long) o).toArray(Long[]::new);
                break;
            case Types.BOOLEAN:
                result = this.list.stream().map(o -> (Boolean) o).toArray(Boolean[]::new);
                break;
            case Types.DOUBLE:
                result = this.list.stream().map(o -> (Double) o).toArray(Double[]::new);
                break;
            case Types.JAVA_OBJECT:
                result = this.list.toArray(new Object[0]);
                break;
            default:
                throw new SQLException(MessageFormat.format(TYPE_NOT_SUPPORTED, this.type));
        }
        
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ListArray && this.list.equals(((ListArray)o).list);
    }

    @Override
    public int hashCode() {
        return Objects.hash(list, type);
    }
}
