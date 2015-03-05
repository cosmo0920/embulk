package org.embulk.spi.util;

import java.util.List;
import org.embulk.config.CommitReport;
import org.embulk.spi.ExecSession;
import org.embulk.spi.ProcessState;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ProcessTask;

public abstract class Executors
{
    private Executors() { }

    public interface ProcessStateCallback
    {
        public void started();

        public void inputCommitted(CommitReport report);

        public void outputCommitted(CommitReport report);

        public void finished();
    }

    public static void process(ExecSession exec,
            ProcessTask task, int taskIndex,
            ProcessStateCallback callback)
    {
        final InputPlugin inputPlugin = exec.newPlugin(InputPlugin.class, task.getInputPluginType());
        final List<FilterPlugin> filterPlugins = Filters.newFilterPlugins(exec, task.getFilterPluginTypes());
        final OutputPlugin outputPlugin = exec.newPlugin(OutputPlugin.class, task.getOutputPluginType());

        TransactionalPageOutput tran = outputPlugin.open(task.getOutputTaskSource(), task.getOutputSchema(), taskIndex);
        PageOutput closeThis = tran;
        callback.started();
        try {
            PageOutput filtered = closeThis = Filters.open(filterPlugins, task.getFilterTaskSources(), task.getFilterSchemas(), tran);

            CommitReport inputCommitReport = inputPlugin.run(task.getInputTaskSource(), task.getInputSchema(), taskIndex, filtered);
            callback.inputCommitted(inputCommitReport);

            CommitReport outputCommitReport = tran.commit();
            tran = null;
            callback.outputCommitted(outputCommitReport);  // TODO check output.finish() is called. wrap or abstract

        } finally {
            try {
                if (tran != null) {
                    tran.abort();
                }
            } finally {
                closeThis.close();
            }
        }
    }

    public static Schema getInputSchema(List<Schema> schemas)
    {
        return schemas.get(0);
    }

    public static Schema getOutputSchema(List<Schema> schemas)
    {
        return schemas.get(schemas.size() - 1);
    }

    public static int getStartedCount(ProcessState state, int taskCount)
    {
        int count = 0;
        for (int i=0; i < taskCount; i++) {
            if (state.isStarted(i)) { count++; }
        }
        return count;
    }

    public static int getFinishedCount(ProcessState state, int taskCount)
    {
        int count = 0;
        for (int i=0; i < taskCount; i++) {
            if (state.isFinished(i)) { count++; }
        }
        return count;
    }
}