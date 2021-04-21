package com.presto.json;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

public class CustomJsonPathType extends AbstractType {

    public static final CustomJsonPathType JSON_PATH = new CustomJsonPathType();
    public static final String NAME = "JsonPathCustom";

    public CustomJsonPathType() {
        super(new TypeSignature(NAME, new TypeSignatureParameter[0]), JsonPathCustom.class);
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry) {
        throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "JsonPath type cannot be serialized");
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries) {
        throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "JsonPath type cannot be serialized");
    }

}
