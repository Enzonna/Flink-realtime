package com.enzo.gmall.realtime.dws.function;

import com.enzo.gmall.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

/**
 * 该注解声明输出的一行中有多少列，列的类型是什么
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String kw) {
        if (kw == null) {
            return;
        }
        Set<String> keywords = KeywordUtil.analyze(kw);
        for (String keyword : keywords) {
            // 将分词向下游传递
            collect(Row.of(keyword));
        }
    }
}
