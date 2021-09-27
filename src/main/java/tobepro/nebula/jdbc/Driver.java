package tobepro.nebula.jdbc;

import tobepro.nebula.jdbc.utils.ExceptionBuilder;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver{
    private static final String JDBC_PREFIX = "jdbc:nebula://"; 
    
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException throwables) {
            throw new RuntimeException("Can't register Nebula driver");
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException(MessageFormat.format("{0} is not a valid url", url));
        }
        String removePrefixUrl = url.replace(JDBC_PREFIX, "");
        String[] hosts = removePrefixUrl.split("/")[0].split(",");
        if (ArrayUtils.isEmpty(hosts)) {
            throw new SQLException(MessageFormat.format("{0} is not a valid url, no host found", url));
        }

        NebulaConfig config = buildConfig(info, removePrefixUrl);
        List<HostAddress> addresses = new ArrayList<>();
        for (String host : hosts) {
            String[] hostInfo = host.split(":");
            if (hostInfo.length != 2) {
                throw new SQLException(MessageFormat.format("{0} is not a valid url, ip or port is missing", url));
            }
            addresses.add(new HostAddress(hostInfo[0], Integer.parseInt(hostInfo[1])));
        }

        NebulaPool pool = new NebulaPool();
        try {
            pool.init(addresses, config);
            config.setUrl(url);
            return NebulaConnection.newInstance(pool, config);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
    
    private NebulaConfig buildConfig(Properties src, String removePrefixUrl) throws SQLException {
        NebulaConfig config = new NebulaConfig();
        src.forEach((key, value) -> {
            if (key.equals("username") || key.equals("user")) {
                config.setUsername((String) value);
            } else if (key.equals("password")) {
                config.setPassword((String) value);
            }
        });
        
        String noPropertyUrl = removeUrlProperties(removePrefixUrl);
        String[] noPropertyUrlArray = noPropertyUrl.split("/");
        if (noPropertyUrlArray.length >= 2) {
            config.setSpace(noPropertyUrlArray[1]);
        }
        
        String[] urlArray = removePrefixUrl.split("\\?");
        if (urlArray.length >= 2) {
            String[] propertyArray = urlArray[1].split("&");
            for (String s : propertyArray) {
                String[] keyValueArray = s.split("=");
                if (keyValueArray.length < 2) {
                    continue;
                }
                switch (keyValueArray[0]) {
                    case "reconnect":
                        config.setReconnect(Boolean.parseBoolean(keyValueArray[1]));
                        break;
                    case "minConnsSize":
                        config.setMinConnSize(Integer.parseInt(keyValueArray[1]));
                        break;
                    case "maxConnsSize":
                        config.setMaxConnSize(Integer.parseInt(keyValueArray[1]));
                        break;
                    case "timeout":
                        config.setTimeout(Integer.parseInt(keyValueArray[1]));
                        break;
                    case "idleTime":
                        config.setIdleTime(Integer.parseInt("idleTime"));
                        break;
                }
            }
        }
        
        if (StringUtils.isBlank(config.getUsername()) || StringUtils.isBlank(config.getPassword())) {
            throw new SQLInvalidAuthorizationSpecException("username or password not provided");
        }
        return config;
    } 

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (Objects.isNull(url)) {
            throw new SQLException("null is not a valid url");
        }
        
        String[] pieces = url.split(":");
        return pieces.length > 3 && url.startsWith(JDBC_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 2;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw ExceptionBuilder.buildUnsupportedOperationException();
    }

    private String removeUrlProperties(String url) {
        String boltUrl = url;
        if (boltUrl.indexOf('?') != -1) {
            boltUrl = url.substring(0, url.indexOf('?'));
        }
        return boltUrl;
    }
}
