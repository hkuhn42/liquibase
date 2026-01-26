package liquibase.command.core.helpers;

import liquibase.Scope;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.core.H2Database;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ChangeSetSqlFileWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesEachChangeSetToSeparateFile() throws Exception {
        Path outputFile = tempDir.resolve("output.sql");
        ChangeSetSqlFileWriter writer = new ChangeSetSqlFileWriter(outputFile.toString());

        DatabaseChangeLog changeLog = new DatabaseChangeLog("changelog.xml");
        ChangeSet changeSet = new ChangeSet("1", "tester", false, false, "changelog.xml", null, null, null);
        changeLog.addChangeSet(changeSet);

        Database database = new H2Database();

        writer.willRun(changeSet, changeLog, database, ChangeSet.RunStatus.NOT_RAN);
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("logging", database);
        executor.comment("test statement");
        writer.ran(changeSet, changeLog, database, ChangeSet.ExecType.EXECUTED);
        writer.close();

        Path changeSetFile = tempDir.resolve("output-1-tester-changelog.xml.sql");
        assertTrue(Files.exists(changeSetFile));
        String content = Files.readString(changeSetFile);
        assertTrue(content.contains("Update Changeset"));
        assertTrue(content.contains("test statement"));
    }
}
