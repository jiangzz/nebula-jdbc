package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.utils.ExceptionBuilder;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class NebulaParameterMetaData implements ParameterMetaData {
    private final NebulaPreparedStatement statement;

    public NebulaParameterMetaData(NebulaPreparedStatement statement) {
        this.statement = statement;
    }

    @Override
    public int getParameterCount() throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeUnknown;
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
