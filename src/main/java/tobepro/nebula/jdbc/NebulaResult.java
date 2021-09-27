package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.utils.ExceptionBuilder;
import tobepro.nebula.jdbc.utils.NebulaUtil;
import com.vesoft.nebula.client.graph.data.ValueWrapper;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class NebulaResult implements ResultSet {
    public static final int DEFAULT_TYPE = TYPE_SCROLL_SENSITIVE;
    public static final int DEFAULT_CONCURRENCY = CONCUR_READ_ONLY;
    public static final int DEFAULT_HOLDABILITY = CLOSE_CURSORS_AT_COMMIT;
    
    private final com.vesoft.nebula.client.graph.data.ResultSet result;
    private final Statement statement;
    private final int type;
    private final int concurrency;
    private final int holdability;
    private final ResultSetMetaData metadata;

    private boolean isClosed = false;
    private int currentRowNumber = 0;
    // 获取列数据为空时将此值置true
    private boolean wasNull = false;
    
    public static ResultSet newInstance(com.vesoft.nebula.client.graph.data.ResultSet result, Statement statement) {
        NebulaResult nebulaResult = new NebulaResult(result, statement);
        return (ResultSet) Proxy.newProxyInstance(NebulaResult.class.getClassLoader(), new Class[] { ResultSet.class }, new NebulaInvocationHandler(nebulaResult));
    }

    private NebulaResult(com.vesoft.nebula.client.graph.data.ResultSet result, Statement statement) {
        this.result = result;
        this.statement = statement;
        this.type = DEFAULT_TYPE;
        this.concurrency = DEFAULT_CONCURRENCY;
        this.holdability = DEFAULT_HOLDABILITY;
        this.metadata = NebulaResultSetMetaData.newInstance(result);
    }

    private void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("ResultSet already closed");
        }
    }

    private ValueWrapper fetchValueFromIndex(int columnIndex) throws SQLException {
        checkClosed();
        com.vesoft.nebula.client.graph.data.ResultSet.Record record = this.result.rowValues(getRow() - 1);
        ValueWrapper valueWrapper = record.get(columnIndex);
        if (valueWrapper.isNull()) {
            this.wasNull = true;
        }
        return valueWrapper;
    }

    private ValueWrapper fetchValueFromLabel(String columnLabel) throws SQLException {
        checkClosed();
        com.vesoft.nebula.client.graph.data.ResultSet.Record record = this.result.rowValues(getRow() - 1);
        ValueWrapper valueWrapper = record.get(columnLabel);
        if (valueWrapper.isNull()) {
            this.wasNull = true;
        }
        return valueWrapper;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }
    
    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return this.holdability;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return this.type;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        if (!result.getColumnNames().contains(columnLabel)) {
            throw new SQLException("Column not present in ResultSet");
        }
        return result.getColumnNames().indexOf(columnLabel) + 1;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return this.concurrency;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.checkClosed();
        if (rows < 0) {
            throw new SQLException("Fetch size must be >= 0");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public boolean next() throws SQLException {
        if (this.result.rowsSize() > currentRowNumber) {
            this.currentRowNumber++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return Wrapper.unwrap(iface, this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return Wrapper.isWrapperFor(iface, this.getClass());
    }

    @Override
    public void close() throws SQLException {
        this.isClosed = true;
    }

    @Override
    public int getRow() throws SQLException {
        return this.currentRowNumber;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.metadata;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return this.wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, String.class);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Boolean.class)).orElse(false);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Boolean.class)).orElse(false);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Integer.class)).orElse(0);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Integer.class)).orElse(0);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Long.class)).orElse(0L);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Long.class)).orElse(0L);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Short.class)).orElse((short) 0);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Short.class)).orElse((short) 0);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Float.class)).orElse(0f);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Float.class)).orElse(0f);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Double.class)).orElse(0d);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return Optional.ofNullable(NebulaUtil.convertToJavaType(valueWrapper, Double.class)).orElse(0d);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, Date.class);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, Date.class);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, Time.class);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, Time.class);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, Timestamp.class);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, Timestamp.class);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, Array.class);
    }
    
    @Override
    public Array getArray(String columnLabel) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, Array.class);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, Object.class);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, Object.class);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, type);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, type);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        String columnName = this.metadata.getColumnName(columnIndex);
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        return NebulaUtil.convertToJavaType(valueWrapper, map.get(columnName));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        return NebulaUtil.convertToJavaType(valueWrapper, map.get(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromLabel(columnLabel);
        if (valueWrapper.isLong()) {
            Long value = NebulaUtil.convertToJavaType(valueWrapper, Long.class);
            return Objects.nonNull(value) ? BigDecimal.valueOf(value) : null;
        } else if (valueWrapper.isDouble()) {
            Double value = NebulaUtil.convertToJavaType(valueWrapper, Double.class);
            return Objects.nonNull(value) ? BigDecimal.valueOf(value) : null;
        } else {
            return null;
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkClosed();
        ValueWrapper valueWrapper = fetchValueFromIndex(columnIndex);
        if (valueWrapper.isLong()) {
            Long value = NebulaUtil.convertToJavaType(valueWrapper, Long.class);
            return Objects.nonNull(value) ? BigDecimal.valueOf(value) : null;
        } else if (valueWrapper.isDouble()) {
            Double value = NebulaUtil.convertToJavaType(valueWrapper, Double.class);
            return Objects.nonNull(value) ? BigDecimal.valueOf(value) : null;
        } else {
            return null;
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return null;
    }

    @Override
    public String toString() {
        try {
            return "NebulaResult:\n" + printResult(this.result);
        } catch (UnsupportedEncodingException e) {
            return "NebulaResult{}";
        }
    }

    private static String printResult(com.vesoft.nebula.client.graph.data.ResultSet resultSet) throws UnsupportedEncodingException {
        StringBuilder builder = new StringBuilder();
        List<String> colNames = resultSet.keys();
        for (String name : colNames) {
            builder.append(String.format("%15s |", name));
        }
        builder.append("\n");

        for (int i = 0; i < resultSet.rowsSize(); i++) {
            com.vesoft.nebula.client.graph.data.ResultSet.Record record = resultSet.rowValues(i);
            for (ValueWrapper value : record.values()) {
                if (value.isLong()) {
                    builder.append(String.format("%15s |", value.asLong()));
                }
                if (value.isBoolean()) {
                    builder.append(String.format("%15s |", value.asBoolean()));
                }
                if (value.isDouble()) {
                    builder.append(String.format("%15s |", value.asDouble()));
                }
                if (value.isString()) {
                    builder.append(String.format("%15s |", value.asString()));
                }
                if (value.isTime()) {
                    builder.append(String.format("%15s |", value.asTime()));
                }
                if (value.isDate()) {
                    builder.append(String.format("%15s |", value.asDate()));
                }
                if (value.isDateTime()) {
                    builder.append(String.format("%15s |", value.asDateTime()));
                }
                if (value.isVertex()) {
                    builder.append(String.format("%15s |", value.asNode()));
                }
                if (value.isEdge()) {
                    builder.append(String.format("%15s |", value.asRelationship()));
                }
                if (value.isPath()) {
                    builder.append(String.format("%15s |", value.asPath()));
                }
                if (value.isList()) {
                    builder.append(String.format("%15s |", value.asList()));
                }
                if (value.isSet()) {
                    builder.append(String.format("%15s |", value.asSet()));
                }
                if (value.isMap()) {
                    builder.append(String.format("%15s |", value.asMap()));
                }
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.currentRowNumber == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.currentRowNumber > this.result.rowsSize();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.currentRowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return this.currentRowNumber == this.result.rowsSize();
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.currentRowNumber = 0;
    }

    @Override
    public void afterLast() throws SQLException {
        this.currentRowNumber = this.result.rowsSize() + 1;
    }

    @Override
    public boolean first() throws SQLException {
        if (this.result.rowsSize() == 0) {
            return false;
        } else {
            this.currentRowNumber = 1;
            return true;
        }
    }

    @Override
    public boolean last() throws SQLException {
        if (this.result.rowsSize() == 0) {
            return false;
        } else {
            this.currentRowNumber = this.result.rowsSize();
            return true;
        }
    }
    
    /*---------------------------------*/
    /*       Not implemented yet       */
    /*---------------------------------*/

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }
    
    @Override
    public byte getByte(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }
}
