package com.ke.bella.workflow.api.callbacks;

import com.ke.bella.queue.TaskWrapper;
import com.ke.bella.workflow.WorkflowCallbackAdaptor;
import com.ke.bella.workflow.WorkflowContext;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
public class WorkflowBatchRunCallback extends WorkflowCallbackAdaptor {
    final Map<String, Object> data = new LinkedHashMap<>();

    private final TaskWrapper task;

    // 最大重试次数
    private static final int MAX_RETRY_COUNT = 3;

    public WorkflowBatchRunCallback(TaskWrapper task) {
        this.task = task;
    }

    @Override
    public void onWorkflowRunSucceeded(WorkflowContext context) {
        synchronized(data) {
            responseWorkflowInfo(context, data);
            responseWorkflowOutputs(context, data);
            responseWorkflowMetaData(context, data);
        }
        Object outputs = data.get("outputs");
        Map<String, Object> result = new HashMap<>();
        result.put("status_code", 200);
        result.put("request_id", task.getTask().getTaskId());
        result.put("body", outputs);
        markCompleteWithRetry(result);
    }

    @Override
    public void onWorkflowRunFailed(WorkflowContext context, String error, Throwable t) {
        synchronized(data) {
            responseWorkflowInfo(context, data);
            responseWorkflowError(context, data, error);
            responseWorkflowMetaData(context, data);
        }
        String errorBody = MapUtils.getString(data, "error", StringUtils.EMPTY);
        Map<String, Object> result = new HashMap<>();
        result.put("status_code", 500);
        result.put("request_id", task.getTask().getTaskId());
        result.put("body", errorBody);
        markCompleteWithRetry(result);
    }

    /**
     * 带重试逻辑的 markComplete 方法封装
     *
     * @param result 要标记完成的结果数据
     */
    private void markCompleteWithRetry(Map<String, Object> result) {
        int retryCount = 0;
        Exception lastException = null;

        while (retryCount <= MAX_RETRY_COUNT) {
            try {
                task.markComplete(result);

                // 成功执行，记录日志
                if (retryCount > 0) {
                    LOGGER.info("markComplete succeeded after {} retries. taskId={} batchId={}",
                            retryCount, task.getTask().getTaskId(), task.getTask().getBatchId());
                }
                return;

            } catch (Exception e) {
                lastException = e;
                retryCount++;

                // 判断是否应该继续重试
                if (retryCount <= MAX_RETRY_COUNT && isRetryableException(e)) {
                    LOGGER.warn("markComplete failed, will retry ({}/{}). taskId={} batchId={}, error: {}",
                            retryCount, MAX_RETRY_COUNT, task.getTask().getTaskId(), task.getTask().getBatchId(), e.getMessage());

                    // 等待一段时间后重试（指数退避）
                    try {
                        long waitTime = (long) Math.pow(2, retryCount - 1) * 100; // 100ms, 200ms, 400ms
                        Thread.sleep(waitTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Retry sleep interrupted. taskId={}", task.getTask().getTaskId(), ie);
                        break;
                    }
                } else {
                    // 不可重试的异常或达到最大重试次数
                    break;
                }
            }
        }

        // 所有重试都失败，记录错误日志
        LOGGER.error("markComplete failed permanently after {} retries. taskId={} batchId={} exception={}",
                retryCount - 1, task.getTask().getTaskId(), task.getTask().getBatchId(), lastException);
    }

    /**
     * 判断异常是否可以重试
     *
     * @param e 异常对象
     * @return true 如果可以重试，false 否则
     */
    private boolean isRetryableException(Exception e) {
        if (e == null) {
            return false;
        }

        String className = e.getClass().getName();
        String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";

        // 网络相关异常
        if (className.contains("IOException") ||
            className.contains("SocketException") ||
            className.contains("ConnectException") ||
            className.contains("UnknownHostException")) {
            return true;
        }

        // 超时异常
        if (className.contains("TimeoutException") ||
            className.contains("SocketTimeoutException")) {
            return true;
        }

        // 根据异常消息判断
        if (message.contains("timeout") ||
            message.contains("connection") ||
            message.contains("temporarily unavailable") ||
            message.contains("service unavailable") ||
            message.contains("too many requests")) {
            return true;
        }

        // 默认不重试
        return false;
    }

    @Override
    public void onWorkflowRunSuspended(WorkflowContext context) {
        Map<String, Object> data = new LinkedHashMap<>();
        responseWorkflowInfo(context, data);
    }
}
