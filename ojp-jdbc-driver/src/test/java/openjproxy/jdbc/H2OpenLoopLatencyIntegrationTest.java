package openjproxy.jdbc;

import lombok.extern.slf4j.Slf4j;
import openjproxy.jdbc.testutil.TestDBUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Slf4j
class H2OpenLoopLatencyIntegrationTest {

    private static final String TABLE_NAME = "h2_open_loop_latency_test";
    private static final int INITIAL_ROWS = 1000;
    private static final int SELECT_QUERY_COUNT = 1000;
    private static final int WRITE_OPERATION_COUNT = 1000;
    private static final int WRITE_OPERATION_TYPE_COUNT = 3;

    private static boolean isH2TestEnabled;
    private Connection connection;

    private enum SqlType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE
    }

    @BeforeAll
    static void setupClass() {
        isH2TestEnabled = Boolean.parseBoolean(System.getProperty("enableH2Tests", "false"));
    }

    @AfterEach
    void tearDown() {
        TestDBUtils.closeQuietly(connection);
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/h2_connection.csv")
    void shouldLogLatencyReportForOpenLoopH2Workload(
            String driverClass, String url, String user, String password) throws SQLException {

        assumeTrue(isH2TestEnabled, "Skipping H2 tests - not enabled");
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new SQLException("JDBC driver class not found: " + driverClass, e);
        }
        connection = DriverManager.getConnection(url, user, password);

        setupSchemaAndSeedRows();

        Map<SqlType, List<Long>> latenciesByType = new EnumMap<>(SqlType.class);
        for (SqlType sqlType : SqlType.values()) {
            latenciesByType.put(sqlType, new ArrayList<>());
        }

        List<Integer> activeIds = new ArrayList<>(INITIAL_ROWS);
        for (int i = 1; i <= INITIAL_ROWS; i++) {
            activeIds.add(i);
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        runSelectQueries(latenciesByType, activeIds, random);
        runWriteQueries(latenciesByType, activeIds, random);

        assertEquals(SELECT_QUERY_COUNT, latenciesByType.get(SqlType.SELECT).size());
        assertEquals(WRITE_OPERATION_COUNT, latenciesByType.get(SqlType.INSERT).size()
                + latenciesByType.get(SqlType.UPDATE).size()
                + latenciesByType.get(SqlType.DELETE).size());
        assertFalse(latenciesByType.get(SqlType.INSERT).isEmpty());
        assertFalse(latenciesByType.get(SqlType.UPDATE).isEmpty());
        assertFalse(latenciesByType.get(SqlType.DELETE).isEmpty());

        logLatencyReport(latenciesByType);
    }

    private void setupSchemaAndSeedRows() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            statement.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR(255))");
        }

        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + TABLE_NAME + " (id, name) VALUES (?, ?)")) {
            for (int i = 1; i <= INITIAL_ROWS; i++) {
                insert.setInt(1, i);
                insert.setString(2, "seed-" + i);
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private void runSelectQueries(Map<SqlType, List<Long>> latenciesByType,
                                  List<Integer> activeIds,
                                  ThreadLocalRandom random) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT name FROM " + TABLE_NAME + " WHERE id = ?")) {
            for (int i = 0; i < SELECT_QUERY_COUNT; i++) {
                int id = activeIds.get(random.nextInt(activeIds.size()));
                long latency = measureLatency(() -> {
                    select.setInt(1, id);
                    try (ResultSet resultSet = select.executeQuery()) {
                        assertTrue(resultSet.next(), "Expected one row for id=" + id);
                    }
                });
                latenciesByType.get(SqlType.SELECT).add(latency);
            }
        }
    }

    private void runWriteQueries(Map<SqlType, List<Long>> latenciesByType,
                                 List<Integer> activeIds,
                                 ThreadLocalRandom random) throws SQLException {
        int nextInsertId = INITIAL_ROWS + 1;
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + TABLE_NAME + " (id, name) VALUES (?, ?)");
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE " + TABLE_NAME + " SET name = ? WHERE id = ?");
             PreparedStatement delete = connection.prepareStatement(
                     "DELETE FROM " + TABLE_NAME + " WHERE id = ?")) {
            for (int i = 0; i < WRITE_OPERATION_COUNT; i++) {
                int operationType = i % WRITE_OPERATION_TYPE_COUNT;
                if (operationType == 0) {
                    int newId = nextInsertId++;
                    long latency = measureLatency(() -> {
                        insert.setInt(1, newId);
                        insert.setString(2, "insert-" + newId);
                        int rows = insert.executeUpdate();
                        assertEquals(1, rows);
                    });
                    activeIds.add(newId);
                    latenciesByType.get(SqlType.INSERT).add(latency);
                } else if (operationType == 1) {
                    int idToUpdate = activeIds.get(random.nextInt(activeIds.size()));
                    long latency = measureLatency(() -> {
                        update.setString(1, "updated-" + idToUpdate);
                        update.setInt(2, idToUpdate);
                        int rows = update.executeUpdate();
                        assertEquals(1, rows);
                    });
                    latenciesByType.get(SqlType.UPDATE).add(latency);
                } else {
                    int deleteIndex = random.nextInt(activeIds.size());
                    int idToDelete = activeIds.remove(deleteIndex);
                    long latency = measureLatency(() -> {
                        delete.setInt(1, idToDelete);
                        int rows = delete.executeUpdate();
                        assertEquals(1, rows);
                    });
                    latenciesByType.get(SqlType.DELETE).add(latency);
                }
            }
        }
    }

    private void logLatencyReport(Map<SqlType, List<Long>> latenciesByType) {
        StringBuilder report = new StringBuilder();
        report.append("\n=== H2 OPEN LOOP LATENCY REPORT ===\n");
        report.append(String.format("SELECT operations: %d%n", latenciesByType.get(SqlType.SELECT).size()));
        report.append(String.format("INSERT operations: %d%n", latenciesByType.get(SqlType.INSERT).size()));
        report.append(String.format("UPDATE operations: %d%n", latenciesByType.get(SqlType.UPDATE).size()));
        report.append(String.format("DELETE operations: %d%n", latenciesByType.get(SqlType.DELETE).size()));
        report.append('\n');

        appendLatencyLine(report, "SELECT", latenciesByType.get(SqlType.SELECT));
        appendLatencyLine(report, "INSERT", latenciesByType.get(SqlType.INSERT));
        appendLatencyLine(report, "UPDATE", latenciesByType.get(SqlType.UPDATE));
        appendLatencyLine(report, "DELETE", latenciesByType.get(SqlType.DELETE));

        log.info(report.toString());
    }

    private void appendLatencyLine(StringBuilder report, String type, List<Long> values) {
        double medianMs = calculateMedianMs(values);
        report.append(String.format("%s median latency: %.3f ms%n", type, medianMs));
    }

    private double calculateMedianMs(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        sorted.sort(Long::compareTo);
        double medianNs = PerformanceMetrics.calculatePercentile(sorted, 50);
        return medianNs / 1_000_000.0;
    }

    @FunctionalInterface
    private interface SqlOperation {
        void run() throws SQLException;
    }

    private long measureLatency(SqlOperation operation) throws SQLException {
        long start = System.nanoTime();
        operation.run();
        return System.nanoTime() - start;
    }
}
