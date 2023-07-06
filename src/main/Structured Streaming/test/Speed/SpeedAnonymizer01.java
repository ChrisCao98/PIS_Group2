package test.Speed;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.math3.distribution.BetaDistribution;



public class SpeedAnonymizer01 implements Serializable, PETProcess<Double> {
    private double min;
    private double max;
    private double gamma;
    private Duration relax = Duration.ofSeconds(1L);
    private double defaultDeviation;
    private double lastValue;
    private Instant lastTime;

    public SpeedAnonymizer01(Double min, Double max, Double gamma, Double defaultDeviation) {
        if (min > max) {
            throw new IllegalArgumentException("min cannot be bigger than max");
        } else if (this.relax == null) {
            throw new IllegalArgumentException("relax cannot be null");
        } else if (gamma <= 1.0) {
            throw new IllegalArgumentException("gamma must be bigger than 1");
        } else if (!this.relax.isNegative() && !this.relax.isZero()) {
            if (defaultDeviation < 0.0) {
                throw new IllegalArgumentException("defaultDeviation cannot be negative");
            } else {
                this.min = min;
                this.max = max;
                this.gamma = gamma;
                this.relax = this.relax;
                this.defaultDeviation = defaultDeviation;
                this.lastValue = -1.0;
                this.lastTime = null;
            }
        } else {
            throw new IllegalArgumentException("relax duration must be positive");
        }
    }

    public ArrayList<Double> process(Double input) {
        return new ArrayList(Arrays.asList(this.processInput(input, Instant.now())));
    }

    public double processInput(double speed, Instant time) {
        Duration sinceLast;
        if (this.lastTime == null) {
            sinceLast = Duration.ofMillis(0L);
        } else {
            sinceLast = Duration.between(this.lastTime, time);
            if (sinceLast.isNegative()) {
                throw new IllegalArgumentException("time argument is before the last time argument.");
            }
        }

        if (this.lastValue < this.min || this.lastValue > this.max) {
            this.lastValue = MathUtilForSpeed.fit(speed, this.min, this.max);
        }

        double factor = (double)sinceLast.toNanos() / (double)this.relax.toNanos();
        double deviation = factor * this.defaultDeviation;
        double lower = Math.max(this.min, this.lastValue - deviation);
        double upper = Math.min(this.max, this.lastValue + deviation);
        double result;
        if (lower != upper) {
            double pivot = MathUtilForSpeed.fit(speed, lower, upper);
            double mode = (pivot - lower) / (upper - lower);
            BetaDistribution distr = MathUtilForSpeed.computeBetaDistribution(mode, this.gamma);
            result = lower + (upper - lower) * distr.sample();
        } else {
            result = upper;
        }

        this.lastValue = result;
        this.lastTime = time;
        return result;
    }

    public double getMin() {
        return this.min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return this.max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public Duration getRelax() {
        return this.relax;
    }

    public void setRelax(Duration relax) {
        this.relax = relax;
    }

    public double getGamma() {
        return this.gamma;
    }

    public void setGamma(double gamma) {
        this.gamma = gamma;
    }

    public double getDefaultDeviation() {
        return this.defaultDeviation;
    }

    public void setDefaultDeviation(double defaultDeviation) {
        this.defaultDeviation = defaultDeviation;
    }

    public double getLastValue() {
        return this.lastValue;
    }

    public void setLastValue(double lastValue) {
        this.lastValue = lastValue;
    }

    public Instant getLastTime() {
        return this.lastTime;
    }

    public void setLastTime(Instant lastTime) {
        this.lastTime = lastTime;
    }

    public static void main(String[] args) {
        double gamma = 10.0;
        SpeedAnonymizer01 sa = new SpeedAnonymizer01(75.0, 83.0, gamma, 2.5);
        double[] sampleValues = new double[]{75.0, 78.0, 81.0, 85.0, 87.0, 89.0, 90.0, 91.0, 91.0, 92.0, 92.0, 92.0, 91.0, 90.0, 88.0, 86.0, 84.0, 83.0, 81.0, 78.0, 76.0};
        double[] generatedValues = new double[sampleValues.length];
        int i = 0;
        double[] var8 = sampleValues;
        int var9 = sampleValues.length;

        for(int var10 = 0; var10 < var9; ++var10) {
            double d = var8[var10];
            ArrayList<Double> result = sa.process(d);
            double tmp = MathUtilForSpeed.round((Double)result.get(0), 2);
            generatedValues[i] = tmp;
            ++i;
        }

        System.out.println();
        System.out.println();
        System.out.println(Arrays.toString(sampleValues));
        System.out.println(Arrays.toString(generatedValues));
    }
}
