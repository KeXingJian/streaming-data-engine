package com.kxj.streamingdataengine.ai;

/**
 * PID控制器
 * 用于平滑参数调整，避免剧烈震荡
 */
public class PIDController {
    private final double kp, ki, kd;
    private final double integralClamp;
    private double integral;
    private double lastError;

    public PIDController(double kp, double ki, double kd) {
        this(kp, ki, kd, Double.MAX_VALUE);
    }

    public PIDController(double kp, double ki, double kd, double integralClamp) {
        this.kp = kp;
        this.ki = ki;
        this.kd = kd;
        this.integralClamp = integralClamp;
    }

    public double calculate(double error) {
        integral += error;
        if (integralClamp != Double.MAX_VALUE) {
            integral = Math.clamp(integral, -integralClamp, integralClamp);
        }
        double derivative = error - lastError;
        lastError = error;
        return kp * error + ki * integral + kd * derivative;
    }

    public void reset() {
        integral = 0;
        lastError = 0;
    }
}
