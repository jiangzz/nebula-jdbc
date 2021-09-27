package tobepro.nebula.jdbc;

import com.vesoft.nebula.Value;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NebulaResultSetMetaData implements ResultSetMetaData {
    private static final Map<Integer, Class<?>> INTERNAL_TYPE_TO_CLASS_MAP = new HashMap<>();
    private static final Map<Integer, Integer> INTERNAL_TYPE_TO_SQL_TYPES_MAP = new HashMap<>();
    private static final Map<Integer, String> INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP = new HashMap<>();
    private final List<String> keys;
    private final ResultSet resultSet;
    
    static {
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.BVAL, Boolean.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.IVAL, Long.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.FVAL, Double.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.SVAL, String.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.DVAL, Date.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.TVAL, Time.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.DTVAL, Timestamp.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.VVAL, Object.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.EVAL, Object.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.PVAL, Object.class);
        INTERNAL_TYPE_TO_CLASS_MAP.put(Value.LVAL, Array.class);
        
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.BVAL, Types.BOOLEAN);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.IVAL, Types.INTEGER);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.FVAL, Types.DOUBLE);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.SVAL, Types.VARCHAR);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.DVAL, Types.DATE);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.TVAL, Types.TIME);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.DTVAL, Types.TIMESTAMP);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.VVAL, Types.JAVA_OBJECT);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.EVAL, Types.JAVA_OBJECT);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.PVAL, Types.JAVA_OBJECT);
        INTERNAL_TYPE_TO_SQL_TYPES_MAP.put(Value.LVAL, Types.ARRAY);

        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.BVAL, "BOOLEAN");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.IVAL, "INTEGER");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.FVAL, "DOUBLE");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.SVAL, "VARCHAR");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.DVAL, "DATE");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.TVAL, "TIME");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.DTVAL, "TIMESTAMP");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.VVAL, "JAVA_OBJECT");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.EVAL, "JAVA_OBJECT");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.PVAL, "JAVA_OBJECT");
        INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.put(Value.LVAL, "ARRAY");
    }
    
    public static ResultSetMetaData newInstance(ResultSet resultSet) {
        NebulaResultSetMetaData metaData = new NebulaResultSetMetaData(resultSet);
        return (ResultSetMetaData) Proxy
                .newProxyInstance(NebulaResultSetMetaData.class.getClassLoader(), new Class[]{ ResultSetMetaData.class }, new NebulaInvocationHandler(metaData));
    }

    private NebulaResultSetMetaData(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.keys = resultSet.keys();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.keys.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        int type = this.getColumnType(column);
        int value = 0;
        if (type == Types.VARCHAR) {
            value = 40;
        } else if (type == Types.INTEGER) {
            value = 10;
        } else if (type == Types.BOOLEAN) {
            value = 5;
        } else if (type == Types.FLOAT) {
            value = 15;
        } else if (type == Types.JAVA_OBJECT) {
            value = 60;
        }
        return value;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return this.getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        if (this.keys == null || column > this.keys.size() || column <= 0) {
            throw new SQLException("Column out of range");
        }
        return this.keys.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return this.resultSet.getSpaceName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        if (this.resultSet.rowsSize() <= 0) {
            return Types.NULL;
        }
        ValueWrapper valueWrapper = this.resultSet.rowValues(0).get(column - 1);
        Integer internalType = valueWrapper.getValue().getSetField();
        if (!INTERNAL_TYPE_TO_SQL_TYPES_MAP.containsKey(internalType)) {
            throw new SQLException(MessageFormat.format("column = {0}, internal type = {1} not support", column, internalType));
        }
        return INTERNAL_TYPE_TO_SQL_TYPES_MAP.get(internalType);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        if (this.resultSet.rowsSize() <= 0) {
            return "";
        }
        ValueWrapper valueWrapper = this.resultSet.rowValues(0).get(column - 1);
        Integer internalType = valueWrapper.getValue().getSetField();
        if (!INTERNAL_TYPE_TO_SQL_TYPES_MAP.containsKey(internalType)) {
            throw new SQLException(MessageFormat.format("column = {0}, internal type = {1} not support", column, internalType));
        }
        return INTERNAL_TYPE_TO_SQL_TYPES_NAME_MAP.get(internalType);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        if (this.resultSet.rowsSize() <= 0) {
            return "";
        }
        ValueWrapper valueWrapper = this.resultSet.rowValues(0).get(column - 1);
        Integer internalType = valueWrapper.getValue().getSetField();
        if (!INTERNAL_TYPE_TO_CLASS_MAP.containsKey(internalType)) {
            throw new SQLException(MessageFormat.format("column = {0}, internal type = {1} not support", column, internalType));
        }
        return INTERNAL_TYPE_TO_CLASS_MAP.get(internalType).toString();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return Wrapper.unwrap(iface, this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return Wrapper.isWrapperFor(iface, this.getClass());
    }
}
