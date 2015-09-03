package org.embulk.spi.util.dynamic;

import java.math.RoundingMode;
import com.google.common.math.DoubleMath;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;

public class LongColumnSetter
        extends AbstractDynamicColumnSetter
{
    public LongColumnSetter(PageBuilder pageBuilder, Column column,
            DefaultValueSetter defaultValue)
    {
        super(pageBuilder, column, defaultValue);
    }

    // Set default rounding mode to keep backward compatibility
    private RoundingMode roundingMode = RoundingMode.HALF_UP;

    public RoundingMode getRoundingMode() {
        return roundingMode;
    }

    public void setRoundingMode(RoundingMode roundingMode) {
        this.roundingMode = roundingMode;
    }

    @Override
    public void setNull()
    {
        pageBuilder.setNull(column);
    }

    @Override
    public void set(boolean v)
    {
        pageBuilder.setLong(column, v ? 1L : 0L);
    }

    @Override
    public void set(long v)
    {
        pageBuilder.setLong(column, v);
    }

    @Override
    public void set(double v)
    {
        long lv;
        try {
            lv = DoubleMath.roundToLong(v, getRoundingMode());
        } catch (ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            defaultValue.setLong(pageBuilder, column);
            return;
        }
        pageBuilder.setLong(column, lv);
    }

    @Override
    public void set(String v)
    {
        long lv;
        try {
            lv = Long.parseLong(v);
        } catch (NumberFormatException e) {
            defaultValue.setLong(pageBuilder, column);
            return;
        }
        pageBuilder.setLong(column, lv);
    }

    @Override
    public void set(Timestamp v)
    {
        pageBuilder.setDouble(column, v.getEpochSecond());
    }
}
