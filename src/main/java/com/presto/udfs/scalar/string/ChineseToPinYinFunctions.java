package com.presto.udfs.scalar.string;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;

@ScalarFunction("pinyin")
public class ChineseToPinYinFunctions {

    private ChineseToPinYinFunctions(){

    }

    @Description("Convert chinese to pinyin.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice convertToPinYin(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return null;
        }

        HanyuPinyinOutputFormat pyFormat = new HanyuPinyinOutputFormat();
        pyFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        pyFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        pyFormat.setVCharType(HanyuPinyinVCharType.WITH_V);

        String result = null;
        try {
            result = PinyinHelper.toHanyuPinyinString(string.toStringUtf8(), pyFormat, "");
            return Slices.utf8Slice(result);
        } catch (BadHanyuPinyinOutputFormatCombination e) {
            return null;
        }
    }
}
