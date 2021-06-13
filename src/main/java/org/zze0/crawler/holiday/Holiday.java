package org.zze0.crawler.holiday;

import lombok.Data;

import java.util.Date;
import java.util.List;

/**
 * 节假日
 *
 * @author Zze0
 * @since 2021/6/13
 */
@Data
public class Holiday {

    /**
     * 年份
     */
    private Integer year;

    /**
     * 日期
     */
    private Date date;

    /**
     * 名称
     */
    private String name;

    /**
     * 补班日期列表
     */
    private List<Date> addWorkDateList;

    /**
     * 假期日期列表
     */
    private List<Date> holidayDateList;
}
