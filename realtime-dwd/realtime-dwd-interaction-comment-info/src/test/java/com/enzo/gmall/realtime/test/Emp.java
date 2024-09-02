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
public class Emp {
    public Integer empno;
    public String ename;
    public Integer deptno;
    public Long ts;
}
