package test.Speed;

import java.text.DecimalFormat;
import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.util.FastMath;

public final class MathUtilForSpeed {
    private MathUtilForSpeed() {
    }

    public static final double entropy(double... probabilities) {
        double sum = 0.0;
        double[] var3 = probabilities;
        int var4 = probabilities.length;

        for(int var5 = 0; var5 < var4; ++var5) {
            double p = var3[var5];
            if (!(p <= 0.0)) {
                sum += p * FastMath.log(2.0, p);
            }
        }

        return -sum;
    }

    public static final double round(double d, int decimals) {
        double r = Math.pow(10.0, (double)decimals);
        return (double)Math.round(d * r) / r;
    }

    public static final String roundToPercent(double d, int decimals) {
        String i = "";

        for(int k = 0; k < decimals; ++k) {
            i = i + "#";
        }

        DecimalFormat decimalFormat = new DecimalFormat("##." + i + "\\%");
        String formatted = decimalFormat.format(d);
        return formatted;
    }

    public static double fit(double d, double min, double max) {
        return Math.min(Math.max(d, min), max);
    }

    public static BetaDistribution computeBetaDistribution(double mode, double gamma) {
        double alpha;
        double beta;
        if (mode <= 0.5) {
            beta = gamma;
            alpha = calcAlpha(mode, gamma);
        } else {
            alpha = gamma;
            beta = calcAlpha(1.0 - mode, gamma);
        }

        return new BetaDistribution(alpha, beta);
    }

    public static double calcAlpha(double mode, double beta) {
        return (beta * mode - 2.0 * mode + 1.0) / (-mode + 1.0);
    }
}

