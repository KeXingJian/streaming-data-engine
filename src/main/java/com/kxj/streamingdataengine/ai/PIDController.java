package com.kxj.streamingdataengine.ai;

/**
 * PID控制器
 * 用于平滑参数调整，避免剧烈震荡
 */
public class PIDController {
    private final double kp, ki, kd;
    private double integral;
    private double lastError;

    public PIDController(double kp, double ki, double kd) {
        this.kp = kp;
        this.ki = ki;
        this.kd = kd;
    }

    public double calculate(double error) {
        integral += error;
        double derivative = error - lastError;
        lastError = error;
        return kp * error + ki * integral + kd * derivative;
    }

    public void reset() {
        integral = 0;
        lastError = 0;
    }
}
