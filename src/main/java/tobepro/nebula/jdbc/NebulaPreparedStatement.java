package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.utils.ExceptionBuilder;
import tobepro.nebula.jdbc.utils.NebulaUtil;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NebulaPreparedStatement extends NebulaAbstractStatement implements PreparedStatement {
    private final Integer parametersNumber;
    private final List<String> parameters;
    private final String rawStatement;
    
    public NebulaPreparedStatement(NebulaConnection nebulaConnection, String rawStatement) {
        super(nebulaConnection);
        this.rawStatement = rawStatement;
        this.parametersNumber = countParameter(rawStatement);
        this.parameters = new ArrayList<>(parametersNumber);
    }

    private int countParameter(String rawStatement) {
        int index = 0;
        String digested = rawStatement;

        String regex = "\\?(?=[^\"]*(?:\"[^\"]*\"[^\"]*)*$)";
        Matcher matcher = Pattern.compile(regex).matcher(digested);

        while (matcher.find()) {
            digested = digested.replaceFirst(regex, "\\$" + index);
            index++;
        }

        return index;
    }
    
    private String buildStatement() {
        int index = 0;
        String digested = this.rawStatement;

        String regex = "\\?(?=[^\"]*(?:\"[^\"]*\"[^\"]*)*$)";
        Matcher matcher = Pattern.compile(regex).matcher(digested);

        while (matcher.find()) {
            digested = digested.replaceFirst(regex, parameters.get(index));
            index++;
        }

        return digested;
    }
    
    private void checkObjectType(Object obj) throws SQLException {
        if (!(obj == null ||
                obj instanceof Boolean ||
                obj instanceof String ||
                obj instanceof Character ||
                obj instanceof Long ||
                obj instanceof Short ||
                obj instanceof Byte ||
                obj instanceof Integer ||
                obj instanceof Double ||
                obj instanceof Float ||
                obj instanceof Iterable ||
                obj instanceof Map ||
                obj instanceof Iterator ||
                obj instanceof boolean[] ||
                obj instanceof String[] ||
                obj instanceof long[] ||
                obj instanceof int[] ||
                obj instanceof double[] ||
                obj instanceof float[])) {
            throw new SQLException("Object of type '" + obj.getClass() + "' isn't supported");
        }
    }
    
    private void checkParamsNumber(int parameterIndex) throws SQLException {
        if (parameterIndex > this.parametersNumber) {
            throw new SQLException("ParameterIndex does not correspond to a parameter marker in the SQL statement");
        }
    }
    
    private void insertParameter(int index, String parameter) {
        this.parameters.add(index - 1, parameter);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        return innerExecute(buildStatement(), result -> {
            ResultSet nebulaResult = NebulaResult.newInstance(result, this);
            this.currentUpdateCount = -1;
            this.currentResultSet = nebulaResult;
            return nebulaResult;
        });
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        return innerExecute(buildStatement(), result -> {
            // Nebula一次只能修改一条
            this.currentUpdateCount = NebulaUtil.calculateUpdateCount(result);
            this.currentResultSet = null;
            return this.currentUpdateCount;
        });
    }
    
    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        return innerExecute(buildStatement(), result -> {
            boolean hasResultSet = !result.isEmpty();
            if (hasResultSet) {
                this.currentResultSet = NebulaResult.newInstance(result, this);
                this.currentUpdateCount = -1;
            } else {
                this.currentResultSet = null;
                this.currentUpdateCount = NebulaUtil.calculateUpdateCount(result);
            }
            return hasResultSet;
        });
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, "NULL");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, x + "");
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, x + "");
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, x + "");
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, x + "");
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, x + "");
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, x + "");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        checkParamsNumber(parameterIndex);
        insertParameter(parameterIndex, "\"" + x + "\"");
    }

    @Override
    public void clearParameters() throws SQLException {
        this.parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        //TODO 解析Object类型
        checkObjectType(x);
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        //TODO 解析Array类型
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return Objects.isNull(this.currentResultSet) ? null : this.currentResultSet.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return new NebulaParameterMetaData(this);
    }

    /*---------------------------------*/
    /*       Not implemented yet       */
    /*---------------------------------*/
    @Override
    public void addBatch() throws SQLException {

    }

    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method execute(String, int) cannot be called on PreparedStatement");
    }

    @Override public boolean execute(String sql) throws SQLException {
        throw new SQLException("Method execute(String) cannot be called on PreparedStatement");
    }

    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method execute(String, int[]) cannot be called on PreparedStatement");
    }

    @Override public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method execute(String, String[]) cannot be called on PreparedStatement");
    }

    @Override public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLException("Method executeQuery(String) cannot be called on PreparedStatement");
    }

    @Override public int executeUpdate(String sql) throws SQLException {
        throw new SQLException("Method executeUpdate(String) cannot be called on PreparedStatement");
    }

    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException("Method executeUpdate(String, int) cannot be called on PreparedStatement");
    }

    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Method executeUpdate(String, int[]) cannot be called on PreparedStatement");
    }

    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLException("Method executeUpdate(String, String[]) cannot be called on PreparedStatement");
    }

    @Override public void setByte(int parameterIndex, byte x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setDate(int parameterIndex, Date x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setTime(int parameterIndex, Time x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType); // simply store a null
    }

    @Override public void setURL(int parameterIndex, URL x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNString(int parameterIndex, String value) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }
}
