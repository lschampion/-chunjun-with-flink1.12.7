/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.lookup;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.enums.ECacheContentType;
import com.dtstack.flinkx.lookup.cache.AbstractSideCache;
import com.dtstack.flinkx.lookup.cache.CacheObj;
import com.dtstack.flinkx.lookup.cache.LRUSideCache;
import com.dtstack.flinkx.lookup.options.LookupOptions;
import com.dtstack.flinkx.metrics.MetricConstant;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author chuixue
 * @create 2021-04-09 14:40
 * @description
 **/
abstract public class BaseLruTableFunction extends AsyncTableFunction<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(BaseLruTableFunction.class);
    /** 和维表join字段的类型 */
    protected final DataType[] keyTypes;
    /** 和维表join字段的名称 */
    protected final String[] keyNames;
    /** 指标 */
    protected transient Counter parseErrorRecords;
    /** 缓存 */
    protected AbstractSideCache sideCache;
    /** 维表配置 */
    protected LookupOptions lookupOptions;
    /** 字段类型 */
    private final String[] fieldsType;
    /** 字段类型 */
    private final String[] fieldsName;
    /** 运行环境 */
    private RuntimeContext runtimeContext;

    private static int TIMEOUT_LOG_FLUSH_NUM = 10;
    private int timeOutNum = 0;

    public BaseLruTableFunction(
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            LookupOptions lookupOptions
    ) {
        this.keyNames = keyNames;
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes =
                Arrays.stream(keyNames)
                        .map(
                                s -> {
                                    checkArgument(
                                            nameList.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            nameList);
                                    return fieldTypes[nameList.indexOf(s)];
                                })
                        .toArray(DataType[]::new);
        this.lookupOptions = lookupOptions;
        this.fieldsName = fieldNames;
        List<String> list = Arrays
                .stream(fieldTypes)
                .map(x -> x.getLogicalType().toString())
                .collect(Collectors.toList());
        this.fieldsType = list.toArray(new String[0]);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        initCache();
        initMetric(context);

        Field field = FunctionContext.class.getDeclaredField("context");
        field.setAccessible(true);
        runtimeContext = (RuntimeContext) field.get(context);

        LOG.info("async dim table config info: {} ", lookupOptions.toString());
    }

    /**
     * 初始化缓存
     */
    private void initCache() {
        if (CacheType.NONE
                .name()
                .equalsIgnoreCase(lookupOptions.getCache())) {
            return;
        }

        if (CacheType.LRU.name().equalsIgnoreCase(lookupOptions.getCache())) {
            sideCache = new LRUSideCache(lookupOptions.getCacheSize(), lookupOptions.getCacheTtl());
        } else {
            throw new RuntimeException(
                    "not support side cache with type:" + lookupOptions.getCache());
        }

        sideCache.initCache();
    }

    /**
     * 初始化Metric
     *
     * @param context 上下文
     */
    private void initMetric(FunctionContext context) {
        parseErrorRecords = context
                .getMetricGroup()
                .counter(MetricConstant.DT_NUM_SIDE_PARSE_ERROR_RECORDS);
    }

    /**
     * 通过key得到缓存数据
     *
     * @param key
     *
     * @return
     */
    protected CacheObj getFromCache(String key) {
        return sideCache.getFromCache(key);
    }

    /**
     * 数据放入缓存
     *
     * @param key
     * @param value
     */
    protected void putCache(String key, CacheObj value) {
        sideCache.putCache(key, value);
    }

    /**
     * 是否开启缓存
     *
     * @return
     */
    protected boolean openCache() {
        return sideCache != null;
    }

    /**
     * 如果缓存获取不到，直接返回空即可，无需判别左/内连接
     *
     * @param future
     */
    public void dealMissKey(CompletableFuture<Collection<RowData>> future) {
        try {
            future.complete(Collections.emptyList());
        } catch (Exception e) {
            dealFillDataError(future, e);
        }
    }

    /**
     * 判断是否需要放入缓存
     *
     * @param key
     * @param missKeyObj
     */
    protected void dealCacheData(String key, CacheObj missKeyObj) {
        if (openCache()) {
            putCache(key, missKeyObj);
        }
    }

    /**
     * 查询前置
     *
     * @param input
     * @param resultFuture
     *
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    protected void preInvoke(Row input, ResultFuture<RowData> resultFuture)
            throws InvocationTargetException, IllegalAccessException {
    }

    /**
     * 异步查询数据
     *
     * @param future 发送到下游
     * @param keys 关联数据
     */
    public void eval(
            CompletableFuture<Collection<RowData>> future,
            Object... keys) throws Exception {

        // preInvoke(future, keys);
        String cacheKey = buildCacheKey(keys);
        // 缓存判断
        if (isUseCache(cacheKey)) {
            invokeWithCache(cacheKey, future);
            return;
        }
        handleAsyncInvoke(future, keys);
    }

    /**
     * 判断缓存是否存在
     *
     * @param cacheKey 缓存健
     *
     * @return
     */
    protected boolean isUseCache(String cacheKey) {
        return openCache() && getFromCache(cacheKey) != null;
    }

    /**
     * 从缓存中获取数据
     *
     * @param cacheKey 缓存健
     * @param future
     */
    private void invokeWithCache(String cacheKey, CompletableFuture<Collection<RowData>> future) {
        if (openCache()) {
            CacheObj val = getFromCache(cacheKey);
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(future);
                    return;
                } else if (ECacheContentType.SingleLine == val.getType()) {
                    try {
                        RowData row = fillData(val.getContent());
                        future.complete(Collections.singleton(row));
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    }
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<RowData> rowList = Lists.newArrayList();
                        for (Object one : (List) val.getContent()) {
                            RowData row = fillData(one);
                            rowList.add(row);
                        }
                        future.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    }
                } else {
                    future.completeExceptionally(new RuntimeException(
                            "not support cache obj type " + val.getType()));
                }
                return;
            }
        }
    }

    /**
     * 请求数据库获取数据
     *
     * @param keys 关联字段数据
     * @param future
     *
     * @throws Exception
     */
    public abstract void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future,
            Object... keys) throws Exception;

    /**
     * 构建缓存key值
     *
     * @param keys
     *
     * @return
     */
    public String buildCacheKey(Object... keys) {
        return Arrays.stream(keys)
                .map(e -> String.valueOf(e))
                .collect(Collectors.joining("_"));
    }

    /**
     * 发送异常
     *
     * @param future
     * @param e
     */
    protected void dealFillDataError(CompletableFuture<Collection<RowData>> future, Throwable e) {
        parseErrorRecords.inc();
        if (parseErrorRecords.getCount() > lookupOptions.getErrorLimit()) {
            LOG.info("dealFillDataError", e);
            future.completeExceptionally(e);
        } else {
            dealMissKey(future);
        }
    }


    protected void preInvoke(CompletableFuture<Collection<RowData>> future, Object... keys)
            throws InvocationTargetException, IllegalAccessException {
        registerTimerAndAddToHandler(future, keys);
    }

    protected void registerTimerAndAddToHandler(
            CompletableFuture<Collection<RowData>> future,
            Object... keys)
            throws InvocationTargetException, IllegalAccessException {
        ScheduledFuture<?> timeFuture = registerTimer(future, keys);
        // resultFuture 是ResultHandler 的实例
        Method setTimeoutTimer = ReflectionUtils.getDeclaredMethod(
                future,
                "setTimeoutTimer",
                ScheduledFuture.class);
        setTimeoutTimer.setAccessible(true);
        setTimeoutTimer.invoke(future, timeFuture);
    }

    protected ScheduledFuture<?> registerTimer(
            CompletableFuture<Collection<RowData>> future,
            Object... keys) {
        long timeoutTimestamp = lookupOptions.getAsyncTimeout()
                + getProcessingTimeService().getCurrentProcessingTime();
        return getProcessingTimeService().registerTimer(
                timeoutTimestamp,
                timestamp -> timeout(future, keys));
    }

    private ProcessingTimeService getProcessingTimeService() {
        // todo 类型不对，暂时无法设置回掉
        return ((StreamingRuntimeContext) this.runtimeContext).getProcessingTimeService();
    }

    // todo 无法设置超时
    public void timeout(
            CompletableFuture<Collection<RowData>> future,
            Object... keys) throws Exception {
        if (timeOutNum % TIMEOUT_LOG_FLUSH_NUM == 0) {
            LOG.info(
                    "Async function call has timed out. input:{}, timeOutNum:{}",
                    keys,
                    timeOutNum);
        }
        timeOutNum++;

        if (timeOutNum > lookupOptions.getErrorLimit()) {
            future.completeExceptionally(
                    new SuppressRestartsException(
                            new Throwable(
                                    String.format(
                                            "Async function call timedOutNum beyond limit. %s",
                                            lookupOptions.getErrorLimit()
                                    )
                            )
                    ));
        } else {
            future.complete(Collections.EMPTY_LIST);
        }
    }

    public RowData fillData(Object sideInput) {
        return fillDataWapper(sideInput, fieldsName, fieldsType);
    }


    /**
     * 资源释放
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     *  fill data
     * @param sideInput
     * @param sideFieldNames
     * @param sideFieldTypes
     * @return
     */
    abstract protected RowData fillDataWapper(
            Object sideInput,
            String[] sideFieldNames,
            String[] sideFieldTypes);
}