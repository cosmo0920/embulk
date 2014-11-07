package org.quickload.record;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.junit.runner.RunWith;
import org.quickload.GuiceJUnitRunner;
import org.quickload.TestUtilityModule;
import org.quickload.buffer.Buffer;
import org.quickload.channel.PageChannel;
import org.quickload.exec.BufferManager;
import org.quickload.exec.ExecModule;

@RunWith(GuiceJUnitRunner.class)
@GuiceJUnitRunner.GuiceModules({ ExecModule.class, TestUtilityModule.class })
public class TestRandomPageBuilderReader
{
    @Inject
    protected BufferManager bufferManager;
    @Inject
    protected RandomSchemaGenerator schemaGen;
    @Inject
    protected RandomRecordGenerator recordGen;

    protected PageChannel channel;
    protected Schema schema;
    protected RandomRecordGenerator gen;
    protected PageBuilder builder;
    protected PageReader reader;

    @Before
    public void setup() throws Exception
    {
        channel = new PageChannel(Integer.MAX_VALUE);
        schema = schemaGen.generate(60);
        builder = new PageBuilder(bufferManager, schema, channel.getOutput());
        reader = new PageReader(schema);
    }

    @After
    public void destroy() throws Exception
    {
        channel.close();
    }

    @Test
    public void testRandomData() throws Exception {
        final List<Record> expected = ImmutableList.copyOf(recordGen.generate(schema, 5000));

        for (final Record record : expected) {
            schema.produce(builder, new RecordProducer()
            {
                @Override
                public void setLong(Column column, LongType.Setter setter)
                {
                    setter.setLong((Long) record.getObject(column.getIndex()));
                }

                @Override
                public void setDouble(Column column, DoubleType.Setter setter)
                {
                    setter.setDouble((Double) record.getObject(column.getIndex()));
                }

                @Override
                public void setString(Column column, StringType.Setter setter)
                {
                    setter.setString((String) record.getObject(column.getIndex()));
                }
            });
            builder.addRecord();
        }
        builder.flush();
        channel.completeProducer();

        List<Record> actual = new ArrayList<Record>();
        for (Object[] values : Pages.toObjects(schema, channel.getInput())) {
            actual.add(new Record(values));
        }
        channel.completeConsumer();

        assertEquals(expected, actual);
    }


}