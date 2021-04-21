package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;

@ScalarFunction(value = "get_serial_n")
public class GetSerialN {

    private GetSerialN(){

    }

    @Description("get serial n")
    @SqlNullable
    @SqlType("array(bigint)")
    public static Block getSerialN(@SqlType(StandardTypes.BIGINT) long n1,
                                   @SqlType(StandardTypes.BIGINT) long n2) {
        if (n1 > n2) {
            return null;
        }

        List<Long> ret = new ArrayList<>((int)(n2-n1+1));
        for (long i = n1; i <= n2; i++) {
            ret.add(i);
        }

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, ret.size());
        ret.stream().forEach(item -> BIGINT.writeLong(blockBuilder, item));

        return blockBuilder;
    }

}
