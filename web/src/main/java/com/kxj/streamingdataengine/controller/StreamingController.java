package com.kxj.streamingdataengine.controller;

import com.kxj.streamingdataengine.service.StreamProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 流处理引擎 Web 控制器
 * 提供 REST API 和页面路由
 */
@Slf4j
@Controller
@RequiredArgsConstructor
public class StreamingController {

    private final StreamProcessingService streamService;

    // ===== 页面路由 =====

    /**
     * 主页 - 流处理演示平台
     */
    @GetMapping("/")
    public String index() {
        log.info("[kxj: 访问主页]");
        return "forward:/index.html";
    }

    // ===== REST API =====

    /**
     * 基础流处理演示
     * GET /api/stream/demo/basic
     */
    @GetMapping("/api/stream/demo/basic")
    @ResponseBody
    public Map<String, Object> basicDemo() {
        log.info("[kxj: API调用 - 基础演示]");
        return streamService.processBasicDemo();
    }

    /**
     * 聚合统计演示
     * GET /api/stream/demo/aggregate/{count}
     */
    @GetMapping("/api/stream/demo/aggregate/{count}")
    @ResponseBody
    public Map<String, Object> aggregateDemo(@PathVariable int count) {
        log.info("[kxj: API调用 - 聚合演示] count={}", count);
        return streamService.processAggregateDemo(count);
    }

    /**
     * IoT传感器数据演示
     * GET /api/stream/demo/iot?sensors=5&readings=100
     */
    @GetMapping("/api/stream/demo/iot")
    @ResponseBody
    public Map<String, Object> iotDemo(
            @RequestParam(defaultValue = "3") int sensors,
            @RequestParam(defaultValue = "50") int readings) {
        log.info("[kxj: API调用 - IoT演示] sensors={}, readings={}", sensors, readings);
        return streamService.processIoTDemo(sensors, readings);
    }

    /**
     * 自定义数据处理
     * POST /api/stream/process
     * Body: [{"value": 10, "name": "item1"}, ...]
     */
    @PostMapping("/api/stream/process")
    @ResponseBody
    public Map<String, Object> processData(@RequestBody List<Map<String, Object>> data) {
        log.info("[kxj: API调用 - 自定义处理] inputCount={}", data.size());
        return streamService.processCustomData(data);
    }

    /**
     * 获取引擎状态
     * GET /api/stream/status
     */
    @GetMapping("/api/stream/status")
    @ResponseBody
    public Map<String, Object> getStatus() {
        return streamService.getEngineStatus();
    }

    /**
     * 获取最近处理结果
     * GET /api/stream/results
     */
    @GetMapping("/api/stream/results")
    @ResponseBody
    public Map<String, Object> getRecentResults() {
        return streamService.getRecentResults();
    }
}
