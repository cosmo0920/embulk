package org.embulk.spi.util;

import java.util.List;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import static junitparams.JUnitParamsRunner.$;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.Buffer;
import org.embulk.spi.util.ListFileInput;
import org.embulk.EmbulkTestRuntime;

@RunWith(Enclosed.class)
public class TestLineDecoder
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testDefaultValues()
    {
        ConfigSource config = Exec.newConfigSource();
        LineDecoder.DecoderTask task = config.loadConfig(LineDecoder.DecoderTask.class);
        assertEquals(StandardCharsets.UTF_8, task.getCharset());
        assertEquals(Newline.CRLF, task.getNewline());
    }

    @Test
    public void testLoadConfig()
    {
        ConfigSource config = Exec.newConfigSource()
            .set("charset", "utf-16")
            .set("newline", "CRLF");
        LineDecoder.DecoderTask task = config.loadConfig(LineDecoder.DecoderTask.class);
        assertEquals(StandardCharsets.UTF_16, task.getCharset());
        assertEquals(Newline.CRLF, task.getNewline());
    }

    private static LineDecoder.DecoderTask getExampleConfig(Charset charset, Newline newline)
    {
        ConfigSource config = Exec.newConfigSource()
            .set("charset", charset)
            .set("newline", newline);
        return config.loadConfig(LineDecoder.DecoderTask.class);
    }

    private static LineDecoder newDecoder(Charset charset, Newline newline, List<Buffer> buffers)
    {
        ListFileInput input = new ListFileInput(ImmutableList.of(buffers));
        return new LineDecoder(input, getExampleConfig(charset, newline));
    }

    private static List<String> doDecode(Charset charset, Newline newline, List<Buffer> buffers)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        LineDecoder decoder = newDecoder(charset, newline, buffers);
        decoder.nextFile();
        while (true) {
            String line = decoder.poll();
            if (line == null) {
                break;
            }
            builder.add(line);
        }
        return builder.build();
    }

    private static List<Buffer> bufferList(Charset charset, String... sources) throws UnsupportedCharsetException
    {
        List<Buffer> buffers = new ArrayList<Buffer>();
        for (String source : sources) {
            ByteBuffer buffer = charset.encode(source);
            buffers.add(Buffer.wrap(buffer.array(), 0, buffer.limit()));
        }

        return buffers;
    }

    private static void assertDoDecode(Charset charset, Newline newline, List<Buffer> source, ImmutableList expected)
    {
        List<String> decoded = doDecode(charset, newline, source);
        assertEquals(expected, decoded);
    }

    @RunWith(JUnitParamsRunner.class)
    public static class LineDecoderParametrizedBasic
    {
        @Rule
        public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

        public Object[] parametersForTestDecodeBasic()
        {
            return $(
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "test1\ntest2\ntest3\n"),
                            ImmutableList.of("test1", "test2", "test3")),
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "てすと1\nテスト2\nてすと3\n"),
                            ImmutableList.of("てすと1", "テスト2", "てすと3")),
                    $(StandardCharsets.UTF_16LE,
                            bufferList(StandardCharsets.UTF_16LE, "てすと1\nテスト2\nてすと3\n"),
                            ImmutableList.of("てすと1", "テスト2", "てすと3")),
                    $(Charset.forName("ms932"),
                            bufferList(Charset.forName("ms932"), "てすと1\r\nテスト2\r\nてすと3\r\n"),
                            ImmutableList.of("てすと1", "テスト2", "てすと3"))
            );
        }

        @Test
        @Parameters(method = "parametersForTestDecodeBasic")
        public void testDecodeBasic(Charset charset, List<Buffer> source, ImmutableList expected) throws Exception
        {
            assertDoDecode(charset, Newline.LF, source, expected);
        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class LineDecoderParametrizedBasicCRLF {
        @Rule
        public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

        public Object[] parametersForTestDecodeBasicCRLF()
        {
            return $(
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "test1\r\ntest2\r\ntest3\r\n"),
                            ImmutableList.of("test1", "test2", "test3")),
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "てすと1\r\nテスト2\r\nてすと3\r\n"),
                            ImmutableList.of("てすと1", "テスト2", "てすと3")),
                    $(StandardCharsets.UTF_16LE,
                            bufferList(StandardCharsets.UTF_16LE, "てすと1\r\nテスト2\r\nてすと3\r\n"),
                            ImmutableList.of("てすと1", "テスト2", "てすと3")),
                    $(Charset.forName("ms932"),
                            bufferList(Charset.forName("ms932"), "てすと1\r\nテスト2\r\nてすと3\r\n"),
                            ImmutableList.of("てすと1", "テスト2", "てすと3"))
            );
        }

        @Test
        @Parameters(method = "parametersForTestDecodeBasicCRLF")
        public void testDecodeBasicCRLF(Charset charset, List<Buffer> source, ImmutableList expected) throws Exception
        {
            assertDoDecode(charset, Newline.CRLF, source, expected);
        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class LineDecoderParametrizedBasicTail
    {
        @Rule
        public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

        public Object[] parametersForTestDecodeBasicTail()
        {
            return $(
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "test1"),
                            ImmutableList.of("test1")),
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "てすと1"),
                            ImmutableList.of("てすと1")),
                    $(StandardCharsets.UTF_16LE,
                            bufferList(StandardCharsets.UTF_16LE, "てすと1"),
                            ImmutableList.of("てすと1")),
                    $(Charset.forName("ms932"),
                            bufferList(Charset.forName("ms932"), "てすと1"),
                            ImmutableList.of("てすと1"))
            );
        }

        @Test
        @Parameters(method = "parametersForTestDecodeBasicTail")
        public void testDecodeBasicTail(Charset charset, List<Buffer> source, ImmutableList expected) throws Exception
        {
            assertDoDecode(charset, Newline.LF, source, expected);
        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class LineDecoderParametrizedBasicChunksLF
    {
        @Rule
        public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

        public Object[] parametersForTestDecodeChunksLF()
        {
            return $(
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "t", "1", "\n", "t", "2"),
                            ImmutableList.of("t1", "t2")),
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "て", "1", "\n", "す", "2"),
                            ImmutableList.of("て1", "す2")),
                    $(StandardCharsets.UTF_16LE,
                            bufferList(StandardCharsets.UTF_16LE, "て", "1", "\n", "す", "2"),
                            ImmutableList.of("て1", "す2")),
                    $(Charset.forName("ms932"),
                            bufferList(Charset.forName("ms932"), "て", "1", "\n", "す", "2"),
                            ImmutableList.of("て1", "す2"))
            );
        }

        @Test
        @Parameters(method = "parametersForTestDecodeChunksLF")
        public void testDecodeBasicChunksLF(Charset charset, List<Buffer> source, ImmutableList expected) throws Exception {
            assertDoDecode(charset, Newline.LF, source, expected);
        }
    }

    @RunWith(JUnitParamsRunner.class)
    public static class LineDecoderParametrizedBasicChunksCRLF
    {
        @Rule
        public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

        public Object[] parametersForTestDecodeChunksCRLF()
        {
            return $(
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "t", "1", "\r\n", "t", "2", "\r", "\n", "t3"),
                            ImmutableList.of("t1", "t2", "t3")),
                    $(StandardCharsets.UTF_8,
                            bufferList(StandardCharsets.UTF_8, "て", "1", "\r\n", "す", "2", "\r", "\n", "と3"),
                            ImmutableList.of("て1", "す2", "と3")),
                    $(StandardCharsets.UTF_16LE,
                            bufferList(StandardCharsets.UTF_16LE, "て", "1", "\r\n", "す", "2", "\r", "\n", "と3"),
                            ImmutableList.of("て1", "す2", "と3")),
                    $(Charset.forName("ms932"),
                            bufferList(Charset.forName("ms932"), "て", "1", "\r\n", "す", "2", "\r", "\n", "と3"),
                            ImmutableList.of("て1", "す2", "と3"))
            );
        }

        @Test
        @Parameters(method = "parametersForTestDecodeChunksCRLF")
        public void testDecodeBasicChunksCRLF(Charset charset, List<Buffer> source, ImmutableList expected) throws Exception
        {
            assertDoDecode(charset, Newline.CRLF, source, expected);
        }
    }
}
