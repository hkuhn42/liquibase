package liquibase.command.core.helpers;

import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.command.CleanUpCommandStep;
import liquibase.command.CommandResultsBuilder;
import liquibase.command.CommandScope;
import liquibase.command.core.UpdateSqlCommandStep;
import liquibase.database.Database;
import liquibase.exception.CommandValidationException;
import liquibase.integration.commandline.LiquibaseCommandLineConfiguration;
import liquibase.util.StringUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Arrays;

/**
 * Switches the output writer for updateSql to write to one file per changeset when enabled.
 */
public class ChangeSetSplitOutputWriterCommandStep extends AbstractHelperCommandStep implements CleanUpCommandStep {

    private ChangeSetSqlFileWriter writer;

    @Override
    public List<Class<?>> requiredDependencies() {
        return Arrays.asList(Database.class, ChangeExecListener.class, Writer.class);
    }

    @Override
    public void validate(CommandScope commandScope) throws CommandValidationException {
        Boolean splitOutput = commandScope.getArgumentValue(UpdateSqlCommandStep.SPLIT_SQL_OUTPUT_ARG);
        String outputFile = LiquibaseCommandLineConfiguration.OUTPUT_FILE.getCurrentValue();
        if (Boolean.TRUE.equals(splitOutput) && StringUtil.isEmpty(outputFile)) {
            throw new CommandValidationException("splitOutput requires --output-file to be set.");
        }
    }

    @Override
    public void run(CommandResultsBuilder resultsBuilder) throws Exception {
        CommandScope commandScope = resultsBuilder.getCommandScope();
        Boolean splitOutput = commandScope.getArgumentValue(UpdateSqlCommandStep.SPLIT_SQL_OUTPUT_ARG);
        if (!Boolean.TRUE.equals(splitOutput)) {
            return;
        }

        String outputFile = LiquibaseCommandLineConfiguration.OUTPUT_FILE.getCurrentValue();
        Writer mainWriter = (Writer) commandScope.getDependency(Writer.class);
        writer = new ChangeSetSqlFileWriter(outputFile, mainWriter);
        ChangeExecListener existingListener = (ChangeExecListener) commandScope.getDependency(ChangeExecListener.class);
        writer.setDelegate(existingListener);
        commandScope.provideDependency(ChangeExecListener.class, writer);
    }

    @Override
    public String[][] defineCommandNames() {
        return new String[][]{UpdateSqlCommandStep.COMMAND_NAME};
    }

    @Override
    public boolean isInternal() {
        return true;
    }

    @Override
    public void cleanUp(CommandResultsBuilder resultsBuilder) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException ignored) {
                // ignore
            }
        }
    }
}
