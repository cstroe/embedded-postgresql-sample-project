package com.github.cstroe.sample;

import org.junit.Test;
import ru.yandex.qatools.embed.postgresql.PostgresExecutable;
import ru.yandex.qatools.embed.postgresql.PostgresProcess;
import ru.yandex.qatools.embed.postgresql.PostgresStarter;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class EmbeddedTest {
    @Test
    public void runEmbedded() throws IOException, SQLException {
        // define of retreive db name and credentials
        final String name = "yourDbname";
        final String username = "yourUser";
        final String password = "youPassword";

        // starting Postgres
        final PostgresStarter<PostgresExecutable, PostgresProcess> runtime = PostgresStarter.getDefaultInstance();
        final PostgresConfig config = PostgresConfig.defaultWithDbName(name, username, password);
        // pass info regarding encoding, locale, collate, ctype, instead of setting global environment settings
        config.getAdditionalInitDbParams().addAll(asList(
                "-E", "UTF-8",
                "--locale=en_US.UTF-8",
                "--lc-collate=en_US.UTF-8",
                "--lc-ctype=en_US.UTF-8"
        ));
        PostgresExecutable exec = runtime.prepare(config);
        PostgresProcess process = exec.start();

        // connecting to a running Postgres
        String url = format("jdbc:postgresql://%s:%s/%s?currentSchema=public&user=%s&password=%s",
                config.net().host(),
                config.net().port(),
                config.storage().dbName(),
                config.credentials().username(),
                config.credentials().password()
        );
        Connection conn = DriverManager.getConnection(url);

        // feeding up the database
        conn.createStatement().execute("CREATE TABLE films (code char(5));");
        conn.createStatement().execute("INSERT INTO films VALUES ('movie');");

        // ... or you can execute SQL files...
        //pgProcess.importFromFile(new File("someFile.sql"))
        // ... or even SQL files with PSQL variables in them...
        //pgProcess.importFromFileWithArgs(new File("someFile.sql"), "-v", "tblName=someTable")

        // performing some assertions
        final Statement statement = conn.createStatement();
        assertThat(statement.execute("SELECT * FROM films;"), is(true));
        assertThat(statement.getResultSet().next(), is(true));

        // close db connection
        conn.close();

        // stop Postgres
        process.stop();

    }
}
