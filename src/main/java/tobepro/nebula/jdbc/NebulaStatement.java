package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.utils.NebulaUtil;

import java.sql.ResultSet;
import java.sql.SQLException;

public class NebulaStatement extends NebulaAbstractStatement {
    private ResultSet currentResultSet;
    private int currentUpdateCount;

    public static NebulaStatement newInstance(NebulaConnection connection) {
        return new NebulaStatement(connection);
    }
    private NebulaStatement(NebulaConnection connection) {
        super(connection);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        return innerExecute(sql, result -> {
            ResultSet nebulaResult = NebulaResult.newInstance(result, this);
            this.currentUpdateCount = -1;
            this.currentResultSet = nebulaResult;
            return nebulaResult;
        });
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        return innerExecute(sql, result -> {
            // Nebula一次只能修改一条
            this.currentUpdateCount = NebulaUtil.calculateUpdateCount(result);
            this.currentResultSet = null;
            return this.currentUpdateCount;
        });
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        return innerExecute(sql, result -> {
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
}
