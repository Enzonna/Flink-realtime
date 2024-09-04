package com.enzo.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * 分词工具类
 */
public class KeywordUtil {
    public static Set<String> analyze(String s) {
        Set<String> result = new HashSet<>();
        // String => Reader

        Reader reader = new StringReader(s);
        // 智能分词
        // max_word
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static void main(String[] args) {
        System.out.println(analyze("小米手机京东5G自营移动联通电信"));
    }
}
