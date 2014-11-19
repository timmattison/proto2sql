package com.timmattison.proto2sql;

import com.google.protobuf.Message;
import com.timmattison.proto2sql.sql.ConvertToPostgresql;
import com.timmattison.proto2sql.sql.ConvertToSql;
import com.timmattison.proto2sql.sql.PostgresqlProtobufPersistence;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

/**
 * Created by timmattison on 11/18/14.
 */
public class TestPostgresqlProtobufPersistence extends TestProtobufPersistence {
    private ConvertToSql convertToSql;

    @Override
    protected void innerTeardown() {

    }

    @Override
    protected void innerSetup() throws Exception {
        convertToSql = new ConvertToPostgresql();

        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerName("localhost");
        dataSource.setDatabaseName("proto2sql");

        createDatabase(dataSource, TestProtobufs.SearchRequest.getDefaultInstance());

        protobufPersistence = new PostgresqlProtobufPersistence(dataSource);
        random = new Random(0);
    }

    private void createDatabase(DataSource dataSource, final Message message) throws Exception {
        JdbcMigration jdbcMigration = new JdbcMigration() {
            @Override
            public void migrate(Connection connection) throws Exception {
                List<String> sql = convertToSql.generateSql(message);

                for (String sqlStatement : sql) {
                    PreparedStatement preparedStatement = connection.prepareStatement(sqlStatement);

                    try {
                        preparedStatement.execute();
                    } finally {
                        preparedStatement.close();
                    }
                }
            }
        };

        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.clean();

        jdbcMigration.migrate(dataSource.getConnection());
    }
}
