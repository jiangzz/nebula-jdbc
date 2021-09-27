package tobepro.nebula.jdbc;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class NebulaArray implements Array {
    public static final List<Integer> SUPPORTED_TYPES = Arrays.asList(Types.VARCHAR, Types.INTEGER, Types.BOOLEAN, Types.DOUBLE, Types.JAVA_OBJECT);
    private static final String NOT_SUPPORTED = "Feature not supported";

    @Override
    public String getBaseTypeName() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public int getBaseType() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Object getArray() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }

    @Override
    public void free() throws SQLException {
        throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
    }
    
    public static int getObjectType(Object obj) {
        int type;

        if(obj instanceof String){
            type = Types.VARCHAR;
        } else if(obj instanceof Long){
            type = Types.INTEGER;
        } else if(obj instanceof Boolean) {
            type = Types.BOOLEAN;
        } else if(obj instanceof Double){
            type = Types.DOUBLE;
        } else {
            type = Types.JAVA_OBJECT;
        }

        return type;
    }
}
