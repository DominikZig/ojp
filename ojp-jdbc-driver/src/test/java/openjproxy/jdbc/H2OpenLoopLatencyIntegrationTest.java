package openjproxy.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
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

/**
 * Open-loop latency integration test for H2 through OJP.
 *
 * <p>This test intentionally acquires and closes a pooled connection for each measured operation
 * to expose potential latency hotspots across connect/execute/close flow steps.
 *
 * <p>It also intentionally enables app-side HikariCP (100/100) as a diagnostics-only exception to
 * the normal "no client-side pool with OJP" guidance, because this test specifically profiles
 * checkout/checkin overhead at fixed concurrency.
 */
@Slf4j
class H2OpenLoopLatencyIntegrationTest {

    private static final String TABLE_NAME = "h2_open_loop_latency_test";
    private static final int INITIAL_ROWS = 1000;
    private static final int SELECT_QUERY_COUNT = 1000;
    private static final int WRITE_OPERATION_COUNT = 1000;
    private static final int WRITE_OPERATION_TYPE_COUNT = 3;
    private static final int HIKARI_POOL_SIZE = 100;

    private static boolean isH2TestEnabled;
    private HikariDataSource dataSource;

    private enum SqlType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE
    }

    private enum StepType {
        CONNECT,
        EXECUTE_QUERY,
        EXECUTE_UPDATE,
        CLOSE
    }

    @BeforeAll
    static void setupClass() {
        isH2TestEnabled = Boolean.parseBoolean(System.getProperty("enableH2Tests", "false"));
    }

    @AfterEach
    void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
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
        dataSource = createHikariDataSource(driverClass, url, user, password);

        setupSchemaAndSeedRows(url, user, password);

        Map<SqlType, List<Long>> sqlLatencies = initializeLatencyMap(SqlType.class);
        Map<StepType, List<Long>> stepLatencies = initializeLatencyMap(StepType.class);

        List<Integer> activeIds = new ArrayList<>(INITIAL_ROWS);
        for (int i = 1; i <= INITIAL_ROWS; i++) {
            activeIds.add(i);
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        runSelectQueries(sqlLatencies, stepLatencies, activeIds, random);
        runWriteQueries(sqlLatencies, stepLatencies, activeIds, random);

        assertEquals(SELECT_QUERY_COUNT, sqlLatencies.get(SqlType.SELECT).size());
        assertEquals(WRITE_OPERATION_COUNT, sqlLatencies.get(SqlType.INSERT).size()
                + sqlLatencies.get(SqlType.UPDATE).size()
                + sqlLatencies.get(SqlType.DELETE).size());
        assertFalse(sqlLatencies.get(SqlType.INSERT).isEmpty());
        assertFalse(sqlLatencies.get(SqlType.UPDATE).isEmpty());
        assertFalse(sqlLatencies.get(SqlType.DELETE).isEmpty());
        assertEquals(SELECT_QUERY_COUNT + WRITE_OPERATION_COUNT, stepLatencies.get(StepType.CONNECT).size());
        assertEquals(SELECT_QUERY_COUNT + WRITE_OPERATION_COUNT, stepLatencies.get(StepType.CLOSE).size());
        assertEquals(SELECT_QUERY_COUNT, stepLatencies.get(StepType.EXECUTE_QUERY).size());
        assertEquals(WRITE_OPERATION_COUNT, stepLatencies.get(StepType.EXECUTE_UPDATE).size());

        logLatencyReport(sqlLatencies, stepLatencies);
    }

    private HikariDataSource createHikariDataSource(String driverClass, String url, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverClass);
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        // Reviewer-requested fixed-size pool to stress checkout/checkin behavior under a large pool.
        config.setMaximumPoolSize(HIKARI_POOL_SIZE);
        config.setMinimumIdle(HIKARI_POOL_SIZE);
        config.setPoolName("H2OpenLoopLatencyPool");
        return new HikariDataSource(config);
    }

    private void setupSchemaAndSeedRows(String url, String user, String password) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            statement.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR(255))");
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
    }

    private void runSelectQueries(Map<SqlType, List<Long>> sqlLatencies,
                                  Map<StepType, List<Long>> stepLatencies,
                                  List<Integer> activeIds,
                                  ThreadLocalRandom random) throws SQLException {
        for (int i = 0; i < SELECT_QUERY_COUNT; i++) {
            int id = activeIds.get(random.nextInt(activeIds.size()));
            withInstrumentedConnection(stepLatencies, connection -> {
                try (PreparedStatement select = connection.prepareStatement(
                        "SELECT name FROM " + TABLE_NAME + " WHERE id = ?")) {
                    select.setInt(1, id);
                    long latency = measureStepLatency(stepLatencies, StepType.EXECUTE_QUERY, () -> {
                        try (ResultSet resultSet = select.executeQuery()) {
                            assertTrue(resultSet.next(), "Expected one row for id=" + id);
                        }
                    });
                    sqlLatencies.get(SqlType.SELECT).add(latency);
                }
            });
        }
    }

    private void runWriteQueries(Map<SqlType, List<Long>> sqlLatencies,
                                 Map<StepType, List<Long>> stepLatencies,
                                 List<Integer> activeIds,
                                 ThreadLocalRandom random) throws SQLException {
        int nextInsertId = INITIAL_ROWS + 1;
        for (int i = 0; i < WRITE_OPERATION_COUNT; i++) {
            int operationType = i % WRITE_OPERATION_TYPE_COUNT;
            if (operationType == 0) {
                int newId = nextInsertId++;
                withInstrumentedConnection(stepLatencies, connection -> {
                    try (PreparedStatement insert = connection.prepareStatement(
                            "INSERT INTO " + TABLE_NAME + " (id, name) VALUES (?, ?)")) {
                        insert.setInt(1, newId);
                        insert.setString(2, "insert-" + newId);
                        long latency = measureStepLatency(stepLatencies, StepType.EXECUTE_UPDATE, () -> {
                            int rows = insert.executeUpdate();
                            assertEquals(1, rows);
                        });
                        sqlLatencies.get(SqlType.INSERT).add(latency);
                    }
                });
                activeIds.add(newId);
            } else if (operationType == 1) {
                int idToUpdate = activeIds.get(random.nextInt(activeIds.size()));
                withInstrumentedConnection(stepLatencies, connection -> {
                    try (PreparedStatement update = connection.prepareStatement(
                            "UPDATE " + TABLE_NAME + " SET name = ? WHERE id = ?")) {
                        update.setString(1, "updated-" + idToUpdate);
                        update.setInt(2, idToUpdate);
                        long latency = measureStepLatency(stepLatencies, StepType.EXECUTE_UPDATE, () -> {
                            int rows = update.executeUpdate();
                            assertEquals(1, rows);
                        });
                        sqlLatencies.get(SqlType.UPDATE).add(latency);
                    }
                });
            } else {
                int deleteIndex = random.nextInt(activeIds.size());
                int idToDelete = activeIds.remove(deleteIndex);
                withInstrumentedConnection(stepLatencies, connection -> {
                    try (PreparedStatement delete = connection.prepareStatement(
                            "DELETE FROM " + TABLE_NAME + " WHERE id = ?")) {
                        delete.setInt(1, idToDelete);
                        long latency = measureStepLatency(stepLatencies, StepType.EXECUTE_UPDATE, () -> {
                            int rows = delete.executeUpdate();
                            assertEquals(1, rows);
                        });
                        sqlLatencies.get(SqlType.DELETE).add(latency);
                    }
                });
            }
        }
    }

    private void logLatencyReport(Map<SqlType, List<Long>> sqlLatencies,
                                  Map<StepType, List<Long>> stepLatencies) {
        StringBuilder report = new StringBuilder();
        report.append("\n=== H2 OPEN LOOP LATENCY REPORT ===\n");
        report.append(String.format("SELECT operations: %d%n", sqlLatencies.get(SqlType.SELECT).size()));
        report.append(String.format("INSERT operations: %d%n", sqlLatencies.get(SqlType.INSERT).size()));
        report.append(String.format("UPDATE operations: %d%n", sqlLatencies.get(SqlType.UPDATE).size()));
        report.append(String.format("DELETE operations: %d%n", sqlLatencies.get(SqlType.DELETE).size()));
        report.append('\n');

        appendLatencyLine(report, "SELECT", sqlLatencies.get(SqlType.SELECT));
        appendLatencyLine(report, "INSERT", sqlLatencies.get(SqlType.INSERT));
        appendLatencyLine(report, "UPDATE", sqlLatencies.get(SqlType.UPDATE));
        appendLatencyLine(report, "DELETE", sqlLatencies.get(SqlType.DELETE));
        report.append('\n');

        report.append("=== STEP LATENCY MEDIANS ===\n");
        appendLatencyLine(report, "connect", stepLatencies.get(StepType.CONNECT));
        appendLatencyLine(report, "executeQuery", stepLatencies.get(StepType.EXECUTE_QUERY));
        appendLatencyLine(report, "executeUpdate", stepLatencies.get(StepType.EXECUTE_UPDATE));
        appendLatencyLine(report, "close", stepLatencies.get(StepType.CLOSE));
        report.append('\n');

        long totalJdbcStepCount = stepLatencies.get(StepType.CONNECT).size()
                + stepLatencies.get(StepType.EXECUTE_QUERY).size()
                + stepLatencies.get(StepType.EXECUTE_UPDATE).size()
                + stepLatencies.get(StepType.CLOSE).size();
        report.append("=== JDBC STEP COUNT (gRPC PARSE OPPORTUNITIES PROXY) ===\n");
        report.append(String.format("Total JDBC step count: %d%n", totalJdbcStepCount));
        report.append("Proxy formula: connect + executeQuery + executeUpdate + close call counts\n");
        report.append("Note: this is not a direct gRPC decoder metric; it approximates the number of JDBC-layer\n");
        report.append("request/response touch points where parse work can happen, and serves as an upper-bound proxy.\n");

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

    private <E extends Enum<E>> Map<E, List<Long>> initializeLatencyMap(Class<E> enumClass) {
        Map<E, List<Long>> map = new EnumMap<>(enumClass);
        for (E value : enumClass.getEnumConstants()) {
            map.put(value, new ArrayList<>());
        }
        return map;
    }

    @FunctionalInterface
    private interface SqlOperation {
        void run() throws SQLException;
    }

    @FunctionalInterface
    private interface ConnectionOperation {
        void run(Connection connection) throws SQLException;
    }

    private long measureStepLatency(Map<StepType, List<Long>> stepLatencies,
                                    StepType stepType,
                                    SqlOperation operation) throws SQLException {
        long start = System.nanoTime();
        operation.run();
        long latency = System.nanoTime() - start;
        stepLatencies.get(stepType).add(latency);
        return latency;
    }

    private void withInstrumentedConnection(Map<StepType, List<Long>> stepLatencies,
                                            ConnectionOperation operation) throws SQLException {
        Connection connection = null;
        try {
            long connectStart = System.nanoTime();
            connection = dataSource.getConnection();
            stepLatencies.get(StepType.CONNECT).add(System.nanoTime() - connectStart);
            operation.run(connection);
        } finally {
            if (connection != null) {
                long closeStart = System.nanoTime();
                connection.close();
                stepLatencies.get(StepType.CLOSE).add(System.nanoTime() - closeStart);
            }
        }
    }
}
