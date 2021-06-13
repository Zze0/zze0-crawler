package org.zze0.crawler.holiday.baidu;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;
import org.zze0.crawler.holiday.Holiday;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 法定节假日爬虫（百度版）
 * //TODO 理论上应该定时一天同步一次，并持久化到数据库，爬虫接口不能强依赖，需考虑planB方案（业务系统可自己修改配置）
 *
 * @author Zze0
 * @since 2021/6/13
 */
@Slf4j
public class BaiDuHolidayCarwler {

    /**
     * 年度法定节假日查询接口地址
     */
    private static final String YEAR_HOLIDAY_URL = "https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?query=法定节假日&resource_id=39042&t=%s&ie=utf8&oe=gbk&format=json&tn=wisetpl&_=%s";

    /**
     * 日历查询接口地址
     */
    private static final String CALENDAR_URL = "https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?query=%s&resource_id=39043&t=%s&ie=utf8&oe=gbk&format=json&tn=wisetpl&_=%s";

    /**
     * http请求工具类
     */
    private static final RestTemplate REST_TEMPLATE = new RestTemplate();

    /**
     * 年度法定节假日（key是年份，value是节假日列表）
     */
    private static final Map<Integer, List<Holiday>> HOLIDAYS = new LinkedHashMap<>();


    /**
     * 初始化年度法定节假日
     *
     * @param startYear 开始年度
     * @param endYear   结束年度
     */
    public static void initHolidays(int startYear, int endYear) {

        Assert.isTrue(startYear <= endYear, "年度入参有误，请检查！开始年度=" + startYear + "; 结束年度=" + endYear);

        long ts = System.currentTimeMillis();

        /*
        查询年度法定节假日列表(查到2050年)，格式如下：
        {
            "status":"0",
            "t":"1623495008826",
            "data":[
                {
                    "holiday":[
                        {
                            "list":[
                                {
                                    "date":"2021-1-1",
                                    "name":"元旦节"
                                },
                                {
                                    "date":"2021-2-11",
                                    "name":"除夕"
                                },
                                {
                                    "date":"2021-2-12",
                                    "name":"春节"
                                },
                                {
                                    "date":"2021-4-4",
                                    "name":"清明节"
                                },
                                {
                                    "date":"2021-5-1",
                                    "name":"劳动节"
                                },
                                {
                                    "date":"2021-6-14",
                                    "name":"端午节"
                                },
                                {
                                    "date":"2021-9-21",
                                    "name":"中秋节"
                                },
                                {
                                    "date":"2021-10-1",
                                    "name":"国庆节"
                                }
                            ],
                            "list#num#baidu":8,
                            "year":"2021"
                        }
                    ]
                }
            ]
        }
         */
        String result = REST_TEMPLATE.getForObject(String.format(YEAR_HOLIDAY_URL, ts, ts), String.class);

        //获取年度节假日列表
        JSONArray yearHolidayArr =
                Optional.ofNullable(result)
                        .filter(StrUtil::isNotBlank)
                        .map(JSON::parseObject)
                        .filter(MapUtil::isNotEmpty)
                        .map(json -> json.getJSONArray("data"))
                        .filter(CollUtil::isNotEmpty)
                        .map(dataJsonArr -> dataJsonArr.getJSONObject(0))
                        .filter(MapUtil::isNotEmpty)
                        .map(dataJson -> dataJson.getJSONArray("holiday"))
                        .filter(CollUtil::isNotEmpty)
                        .orElseThrow(() -> new IllegalArgumentException("年度法定节假日查询失败"));

        //----------------下面定位本节假日在日历中的下标位位置--------------------

        //年份获取函数
        Function<JSONObject, Integer> yearGetter = yearHoliday -> yearHoliday.getInteger("year");

        //年度法定节假日列表中第一个年份
        int firstYear =
                Optional.of(yearHolidayArr)
                        .map(arr -> arr.getJSONObject(0))
                        .map(yearGetter)
                        .orElseThrow(() -> new IllegalArgumentException("年度法定节假日列表中第一个年份获取失败"));

        if (startYear < firstYear) {
            startYear = firstYear;
        }

        for (int index = startYear - firstYear; index < yearHolidayArr.size() && firstYear + index <= endYear; index++) {

            JSONObject yearHoliday = yearHolidayArr.getJSONObject(index);
            if (MapUtil.isEmpty(yearHoliday)) {
                continue;
            }

            Integer year = yearGetter.apply(yearHoliday);
            if (null == year) {
                log.warn("年份获取失败：{}", yearHoliday);
                continue;
            }

            //本年度的节假日列表
            JSONArray holidayInfoArr = yearHoliday.getJSONArray("list");
            if (CollUtil.isEmpty(holidayInfoArr)) {
                log.warn("{}年没有节假日：{}", year, yearHoliday);
                continue;
            }

            //本年度的节假日个数（其实根据"list#num#baidu"key也可取到）
            //long holidayCount = holidayInfoArr.size();

            for (int holidayIndex = 0; holidayIndex < holidayInfoArr.size(); holidayIndex++) {

                //节假日详情
                JSONObject holidayInfo = holidayInfoArr.getJSONObject(holidayIndex);
                if (MapUtil.isEmpty(holidayInfo)) {
                    log.warn("{}年的第{}个节假日是空的？{}", year, holidayIndex, yearHoliday);
                    continue;
                }

                //节假日日期
                Date date = DateUtil.parse(holidayInfo.getString("date"));
                if (null == date) {
                    log.warn("{}年的第{}个节假日日期是空的？{}", year, holidayIndex, holidayInfo);
                    continue;
                }

                //节假日名称
                String name = holidayInfo.getString("name");
                if (StrUtil.isBlank(name)) {
                    log.warn("{}年的第{}个节假日名称是空的？{}", year, holidayIndex, holidayInfo);
                    continue;
                }
                if ("除夕".equals(StrUtil.trim(name))) {
                    //忽略“除夕”节假日，因为会和“春节”重复
                    continue;
                }

                Holiday holiday = new Holiday();
                holiday.setDate(date);
                holiday.setYear(year);
                holiday.setName(name);

                //补全节假日详情（如 假期、补班日 等等）
                initHolidayDetails(holiday);

                //TODO 持久化到数据库
                HOLIDAYS.computeIfAbsent(year, y -> new ArrayList<>())
                        .add(holiday);
            }
        }
    }

    /**
     * 补全节假日详情（如 假期、补班日 等等）
     *
     * @param holiday 节假日
     */
    private static void initHolidayDetails(Holiday holiday) {

        long ts = System.currentTimeMillis();

        Date holidayDate = holiday.getDate();

        /*
         查询日历信息(指定月份查询，会查出来前后共90天左右的数据)，格式如下：
         {
            "status":"0",
            "t":"1623499626147",
            "data":[
                {
                    "almanac":[
                        {
                            "animal":"牛",
                            "avoid":"搬家.装修.开业.入宅.开工.动土.出行.安葬.上梁.开张.旅游.破土.修造.开市.纳财.移徙.立券.竖柱.放水.分居.行丧.开仓.置产.筑堤.出货",
                            "cnDay":"六",
                            "day":"1",
                            "desc":"劳动节",
                            "gzDate":"己酉",
                            "gzMonth":"壬辰",
                            "gzYear":"辛丑",
                            "isBigMonth":"1",
                            "lDate":"二十",
                            "lMonth":"三",
                            "lunarDate":"20",
                            "lunarMonth":"3",
                            "lunarYear":"2021",
                            "month":"5",
                            "oDate":"2021-04-30T16:00:00.000Z",
                            "status":"1",
                            "suit":"结婚.领证.订婚.求嗣.修坟.赴任.祈福.祭祀.纳畜.启钻.捕捉.嫁娶.纳采.盖屋.栽种.斋醮.招赘.纳婿.藏宝",
                            "term":"",
                            "type":"i",
                            "value":"劳动节",
                            "year":"2021"
                        }
                    ]
                }
            ]
        }
         */
        String yearMonth = new SimpleDateFormat("yyyy年M月").format(holidayDate);
        String result = REST_TEMPLATE.getForObject(String.format(CALENDAR_URL, yearMonth, ts, ts), String.class);


        //获取日历信息列表
        JSONArray almanacArr =
                Optional.ofNullable(result)
                        .filter(StrUtil::isNotBlank)
                        .map(JSON::parseObject)
                        .filter(MapUtil::isNotEmpty)
                        .map(json -> json.getJSONArray("data"))
                        .filter(CollUtil::isNotEmpty)
                        .map(dataJsonArr -> dataJsonArr.getJSONObject(0))
                        .filter(MapUtil::isNotEmpty)
                        .map(dataJson -> dataJson.getJSONArray("almanac"))
                        .filter(CollUtil::isNotEmpty)
                        .orElseThrow(() -> new IllegalArgumentException(yearMonth + "日历信息查询失败"));

        //----------------下面定位本节假日在日历中的下标位位置--------------------

        int almanacCount = almanacArr.size();

        //日历列表第一天日期
        Date firstAlmanacDate = getAlmanac(almanacArr, 0, (almanac, almanacDate) -> almanacDate);

        //本节假日所在日历中的下标位（后面根据这个下标位前后推算哪些是假期或者补班日）
        int holidayIndex = (int) DateUtil.between(firstAlmanacDate, holidayDate, DateUnit.DAY, true);
        Assert.isTrue(
                holidayIndex < almanacCount && DateUtil.isSameDay(getAlmanac(almanacArr, holidayIndex, (almanac, almanacDate) -> almanacDate), holidayDate),
                "未在日历中查找到节假日日期！" + DateUtil.formatDate(holidayDate));

//        //----------------下面使用二分查找算法，快速定位本节假日位置--------------------
//
//        long holidayTs = holidayDate.getTime();
//
//        //本节假日所在日历中的下标位（后面根据这个下标位前后推算哪些是假期或者补班日）
//        int holidayIndex = -1;
//
//        //查找范围起点
//        int start = 0;
//        //查找范围终点
//        int end = almanacCount - 1;
//        //查找范围中位数
//        int mid;
//
//        while (start <= end) {
//            //折半，取中间点
//            mid = start + (end - start) / 2;
//
//            //从日历信息中提取中间位置日期的时间戳
//            long midTs = getAlmanac(almanacArr, mid, (almanac, almanacDate) -> almanacDate.getTime());
//
//            if (holidayTs < midTs) {
//                //下次查找从左半边继续折半查找
//                end = mid - 1;
//            } else if (holidayTs > midTs) {
//                //下次查找从右半边继续折半查找
//                start = mid + 1;
//            } else {
//                //刚巧中间位置就是节假日日期
//                holidayIndex = mid;
//                break;
//            }
//        }
//
//        Assert.isTrue(holidayIndex >= 0, "未在日历中查找到节假日日期！" + DateUtil.formatDate(holidayDate));

        //----------------下面分析哪些是本节假日的假期、补班日--------------------

        //补班日期列表
        List<Date> addWorkDateList = new ArrayList<>();

        //假期日期列表
        List<Date> holidayDateList = new ArrayList<>();

        //假期是否连续（用来判断是不是同一个假期周期）
        AtomicBoolean holidayContinuous = new AtomicBoolean(true);

        //周末计数（用来分析补班日时截至时间的辅助参数）
        AtomicInteger weekendCount = new AtomicInteger(0);

        //日历分析器，分析哪些是本节假日的假期、补班日。返回true表示需要继续分析，返回false将中断后续分析
        Function<Integer, Boolean> analyzer =
                index ->
                        getAlmanac(almanacArr, index, (almanac, almanacDate) -> {
                            int status = Optional.ofNullable(almanac.getInteger("status")).orElse(-1);

                            if (holidayContinuous.get()) {
                                if (1 == status) {
                                    //记录假期
                                    holidayDateList.add(almanacDate);
                                } else {
                                    //假期不再连续时，设置中断标识
                                    holidayContinuous.set(false);

                                    if (2 == status) {
                                        //记录补班
                                        addWorkDateList.add(almanacDate);
                                    }
                                }
                            } else {
                                if (1 == status) {
                                    //如果是遇到另一个新假期，需要判断补班日的节假日归属问题，然后可以中断继续查找了

                                    Optional.of(holidayDateList)
                                            .map(list -> list.get(list.size() - 1))
                                            .ifPresent(lastHolidayDate -> {

                                                ListIterator<Date> iterator = addWorkDateList.listIterator(addWorkDateList.size());

                                                while (iterator.hasPrevious()) {

                                                    //补班日期
                                                    Date addWorkDate = iterator.previous();

                                                    //补班日和最后一天假期的天数偏移量
                                                    long addWorkDayOffset = DateUtil.between(addWorkDate, lastHolidayDate, DateUnit.DAY, true);

                                                    //补班日和新假期的天数偏移量
                                                    long addWorkDayOffset2NewHoliday = DateUtil.between(addWorkDate, almanacDate, DateUnit.DAY, true);

                                                    if (addWorkDayOffset > addWorkDayOffset2NewHoliday) {

                                                        //另一个新假期离补班日更近，那这个补班日应该是属于它的，而不是属于当前节假日的。
                                                        iterator.remove();

                                                    } else if (addWorkDayOffset == addWorkDayOffset2NewHoliday) {

                                                        //补班日离当前假期、新假期的偏移量一致，就按照优先分配给后面新假期的原则
                                                        if (addWorkDate.before(lastHolidayDate)) {
                                                            break;
                                                        } else {
                                                            iterator.remove();
                                                        }

                                                    } else {

                                                        //补班日离当前假期更近，可以不用再继续判断补班日的节假日归属问题
                                                        break;
                                                    }
                                                }
                                            });

                                    return false;

                                } else if (2 == status) {
                                    //记录补班
                                    addWorkDateList.add(almanacDate);
                                }

                                Week week = DateUtil.dayOfWeekEnum(almanacDate);
                                if (Week.SATURDAY.equals(week) || Week.SUNDAY.equals(week)) {
                                    //过了两个周末了，可以中断继续查找补班日了
                                    return weekendCount.incrementAndGet() < 4;
                                }
                            }
                            return true;
                        });

        //从节假日日期往前查找，看看节假日前面有多少天是假期，多少天是补班日
        for (int index = holidayIndex; index > 0; index--) {
            if (!analyzer.apply(index)) {
                break;
            }
        }

        //往前查找的日期，要把顺序反回来才是按时间先后排序
        holidayDateList.sort(Date::compareTo);
        addWorkDateList.sort(Date::compareTo);

        //重置标识
        holidayContinuous.set(true);
        weekendCount.set(0);

        //从节假日日期往后查找，看看节假日后面有多少天是假期，多少天是补班日
        for (int index = holidayIndex + 1; index < almanacCount; index++) {
            if (!analyzer.apply(index)) {
                break;
            }
        }

        holiday.setHolidayDateList(holidayDateList);
        holiday.setAddWorkDateList(addWorkDateList);

    }

    /**
     * 从日历信息列表提取某个日期的信息，并根据mapper转换成特定的类型返回
     *
     * @param almanacArr 日历信息列表
     * @param index      日历日期下标位
     * @param mapper     转换器（入参1:日历信息；入参2:日期）
     * @param <T>        转换器输出值类型
     * @return 转换器输出值
     */
    private static <T> T getAlmanac(JSONArray almanacArr, int index, BiFunction<JSONObject, Date, T> mapper) {
        JSONObject almanac = almanacArr.getJSONObject(index);
        Assert.notEmpty(almanac, "日历中第" + index + "个日期为空？");

        String almanacStr = almanac.toString();

        //年
        Integer year = almanac.getInteger("year");
        Assert.notNull(year, "日历中第" + index + "个日期年份为空？" + almanacStr);

        //月
        Integer month = almanac.getInteger("month");
        Assert.notNull(month, "日历中第" + index + "个日期月份为空？" + almanacStr);

        //日
        Integer day = almanac.getInteger("day");
        Assert.notNull(day, "日历中第" + index + "个日期日份为空？" + almanacStr);

        try {
            Date date = new SimpleDateFormat("yyyy-M-d").parse(year + "-" + month + "-" + day);
            return mapper.apply(almanac, date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        initHolidays(2020, 2021);
        //System.out.println(JSON.toJSONStringWithDateFormat(HOLIDAYS, "yyyy-MM-dd", SerializerFeature.PrettyFormat));

        HOLIDAYS.forEach((year, holidays) -> {
            System.out.printf("\n【%d年度】----------------------------------------------------------------------------------------------------------------------------------\n", year);
            for (Holiday holiday : holidays) {
                System.out.printf("\n%s 放假 %d 天，补班 %d 天：", holiday.getName(), holiday.getHolidayDateList().size(), holiday.getAddWorkDateList().size());
                System.out.printf("\n       放假：%s", holiday.getHolidayDateList().stream().map(DateUtil::formatDate).collect(Collectors.joining("、")));
                System.out.printf("\n       补班：%s\n", holiday.getAddWorkDateList().stream().map(DateUtil::formatDate).collect(Collectors.joining("、")));
            }
            System.out.println("\n* 国庆节和中秋节有时会合在一起放假，如果发现中秋节和国庆节放假、补班情况一致，习惯就好。");
            System.out.println("-------------------------------------------------------------------------------------------------------------------------------------------");
        });
    }
}
