package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.utils.ExceptionBuilder;
import tobepro.nebula.jdbc.utils.NebulaUtil;

import java.sql.*;
import java.util.List;
import java.util.function.Function;

public abstract class NebulaAbstractStatement implements Statement {
    protected NebulaConnection connection;
    protected Integer currentUpdateCount;
    protected ResultSet currentResultSet;
    protected Integer maxRows;
    protected Integer queryTimeout;
    protected List<String> batchStatements;
    
    protected NebulaAbstractStatement(NebulaConnection nebulaConnection) {
        this.connection = nebulaConnection;
    }

    protected void checkClosed() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Statement already closed");
        }
    }
    
    protected <T> T innerExecute(String sql, Function<com.vesoft.nebula.client.graph.data.ResultSet, T> fun) throws SQLException {
        checkClosed();
        return NebulaUtil.execute(this, sql, fun);
    }

    @Override
    public Connection getConnection() throws SQLException {
        this.checkClosed();
        return this.connection;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        this.checkClosed();
        int update = this.currentUpdateCount;

        if (this.currentResultSet != null) {
            update = -1;
        } else {
            this.currentUpdateCount = -1;
        }
        return update;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        this.checkClosed();
        ResultSet resultSet = this.currentResultSet;
        this.currentResultSet = null;
        return resultSet;
    }

    @Override
    public int getMaxRows() throws SQLException {
        this.checkClosed();
        return this.maxRows;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !(connection != null && !connection.isClosed());
    }

    @Override
    public void close() throws SQLException {
        if (!this.isClosed()) {
            if (this.currentResultSet != null && !this.currentResultSet.isClosed()) {
                this.currentResultSet.close();
            }
            this.currentUpdateCount = -1;
            this.connection = null;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return tobepro.nebula.jdbc.Wrapper.unwrap(iface, this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return Wrapper.isWrapperFor(iface, this.getClass());
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.checkClosed();
        if (rows != Integer.MIN_VALUE && (this.getMaxRows() > 0 && rows > this.getMaxRows())) {
            throw new UnsupportedOperationException("Not implemented yet. => maxRow :" + getMaxRows() + " rows :" + rows);
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        this.checkClosed();
        return this.currentResultSet != null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        this.checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.checkClosed();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.checkClosed();
        this.batchStatements.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        this.checkClosed();
        this.batchStatements.clear();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return NebulaResult.DEFAULT_CONCURRENCY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return NebulaResult.DEFAULT_TYPE;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return NebulaResult.DEFAULT_HOLDABILITY;
    }

    /*---------------------------------*/
    /*       Not implemented yet       */
    /*---------------------------------*/

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }
}
