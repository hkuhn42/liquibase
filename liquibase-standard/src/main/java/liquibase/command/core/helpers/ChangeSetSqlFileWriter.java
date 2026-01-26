package liquibase.command.core.helpers;

import liquibase.Scope;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.ChangeSet.ExecType;
import liquibase.changelog.ChangeSet.RunStatus;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.change.Change;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.executor.LoggingExecutor;
import liquibase.util.StringUtil;
import liquibase.util.LoggingExecutorTextUtil;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * ChangeExecListener that swaps the LoggingExecutor output per changeset.
 */
public class ChangeSetSqlFileWriter implements ChangeExecListener, Closeable {
    private final Path outputPath;
    private ChangeExecListener delegate;
    private Writer changeSetWriter;
    private LoggingExecutor loggingExecutor;
    private OutputStream fallbackOutputStream;

    public ChangeSetSqlFileWriter(String outputFilePath) throws IOException {
        this.outputPath = Path.of(outputFilePath).toAbsolutePath();
        if (outputPath.getParent() != null) {
            Files.createDirectories(outputPath.getParent());
        }
    }

    public void setDelegate(ChangeExecListener delegate) {
        this.delegate = delegate;
    }

    public OutputStream getFallbackOutputStream() throws IOException {
        if (fallbackOutputStream == null) {
            fallbackOutputStream = new FileOutputStream(outputPath.toFile(), false);
        }
        return fallbackOutputStream;
    }

    @Override
    public void willRun(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, RunStatus runStatus) {
        try {
            startChangeSetOutput(changeSet, database);
        } catch (IOException | DatabaseException e) {
            throw new RuntimeException("Unable to open changeset SQL output file", e);
        }
        if (delegate != null) {
            delegate.willRun(changeSet, databaseChangeLog, database, runStatus);
        }
    }

    @Override
    public void ran(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, ExecType execType) {
        if (delegate != null) {
            delegate.ran(changeSet, databaseChangeLog, database, execType);
        }
        closeChangeSetOutput();
    }

    @Override
    public void willRollback(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database) {
        if (delegate != null) {
            delegate.willRollback(changeSet, databaseChangeLog, database);
        }
    }

    @Override
    public void rolledBack(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database) {
        if (delegate != null) {
            delegate.rolledBack(changeSet, databaseChangeLog, database);
        }
    }

    @Override
    public void preconditionFailed(liquibase.exception.PreconditionFailedException error,
                                   liquibase.precondition.core.PreconditionContainer.FailOption onFail) {
        if (delegate != null) {
            delegate.preconditionFailed(error, onFail);
        }
    }

    @Override
    public void preconditionErrored(liquibase.exception.PreconditionErrorException error,
                                    liquibase.precondition.core.PreconditionContainer.ErrorOption onError) {
        if (delegate != null) {
            delegate.preconditionErrored(error, onError);
        }
    }

    @Override
    public void willRun(Change change, ChangeSet changeSet, DatabaseChangeLog changeLog, Database database) {
        if (delegate != null) {
            delegate.willRun(change, changeSet, changeLog, database);
        }
    }

    @Override
    public void ran(Change change, ChangeSet changeSet, DatabaseChangeLog changeLog, Database database) {
        if (delegate != null) {
            delegate.ran(change, changeSet, changeLog, database);
        }
    }

    @Override
    public void runFailed(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Exception exception) {
        if (delegate != null) {
            delegate.runFailed(changeSet, databaseChangeLog, database, exception);
        }
        closeChangeSetOutput();
    }

    @Override
    public void rollbackFailed(ChangeSet changeSet, DatabaseChangeLog databaseChangeLog, Database database, Exception exception) {
        if (delegate != null) {
            delegate.rollbackFailed(changeSet, databaseChangeLog, database, exception);
        }
    }

    private void startChangeSetOutput(ChangeSet changeSet, Database database) throws IOException, DatabaseException {
        closeChangeSetOutput();
        Path filePath = getChangeSetOutputPath(changeSet);
        if (filePath.getParent() != null) {
            Files.createDirectories(filePath.getParent());
        }
        changeSetWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath.toFile(), false),
                liquibase.GlobalConfiguration.OUTPUT_FILE_ENCODING.getCurrentValue()));
        Executor jdbcExecutor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        loggingExecutor = new LoggingExecutor(jdbcExecutor, changeSetWriter, database);
        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor("jdbc", database, loggingExecutor);
        Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor("logging", database, loggingExecutor);
        LoggingExecutorTextUtil.outputHeader("Update Changeset " + changeSet.toString(false), database,
                changeSet.getChangeLog() != null ? changeSet.getChangeLog().getFilePath() : changeSet.getFilePath());
    }

    private Path getChangeSetOutputPath(ChangeSet changeSet) throws IOException {
        String baseName = stripExtension(outputPath.getFileName().toString());
        String safeId = sanitize(changeSet.getId());
        String safeAuthor = sanitize(changeSet.getAuthor());
        String safeFile = sanitize(changeSet.getFilePath());
        String fileName = String.format("%s-%s-%s-%s.sql", baseName, safeId, safeAuthor, safeFile);
        return outputPath.resolveSibling(fileName);
    }

    private String stripExtension(String fileName) {
        int index = fileName.lastIndexOf('.');
        if (index <= 0) {
            return fileName;
        }
        return fileName.substring(0, index);
    }

    private String sanitize(String value) {
        if (StringUtil.isEmpty(value)) {
            return "unknown";
        }
        String normalized = value.replace(File.separatorChar, '-');
        return normalized.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private void closeChangeSetOutput() {
        if (changeSetWriter != null) {
            try {
                changeSetWriter.flush();
                changeSetWriter.close();
            } catch (IOException ignored) {
                // ignore
            }
            changeSetWriter = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeChangeSetOutput();
        if (fallbackOutputStream != null) {
            fallbackOutputStream.close();
        }
    }
}
