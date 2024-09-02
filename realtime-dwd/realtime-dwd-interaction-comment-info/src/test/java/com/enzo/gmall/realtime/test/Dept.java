package com.enzo.gmall.realtime.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Felix
 * @date 2024/8/17
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Dept {
    public Integer deptno;
    public String dname;
    public Long ts;
}
