import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class UnitTest {
    @Test
    public void doTest() throws Exception {
        Class.forName("tobepro.nebula.jdbc.Driver");
        Properties properties = new Properties();
        properties.setProperty("username", "nebula");
        properties.setProperty("password", "nebula");
        Connection conn = DriverManager.getConnection("jdbc:nebula://10.1.0.2:9669,10.1.0.3:9669,10.1.0.167:4/test", properties);
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("MATCH (m:movie) return m LIMIT 10");
    }
}
