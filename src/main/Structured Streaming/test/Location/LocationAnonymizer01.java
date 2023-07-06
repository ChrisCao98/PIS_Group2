package test.Location;


import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.math3.distribution.BetaDistribution;
import scala.Tuple2;
import pis.group2.Utils.MathUtilForLocation;
import test.Speed.PETProcess;

public class LocationAnonymizer01 implements PETProcess<Tuple2<Double, Double>> {
    private List<LocationWrapper> locations;
    private final int candidateAmount;
    private final List<Double> intervals;
    private final int k;
    private final int m;
    private final double gamma;

    public LocationAnonymizer01(List<Point2D.Double> locations, int k, int m, double gamma) {
        this.locations = (List)locations.stream().map((l) -> {
            return new LocationWrapper(l);
        }).collect(Collectors.toCollection(ArrayList::new));
        this.candidateAmount = this.locations.size() - 1;
        this.intervals = this.initIntervals();
        this.k = k;
        this.m = m;
        this.gamma = gamma;
    }

    public LocationAnonymizer01(Integer k, Integer m, Double gamma) {
        LocationWrapper[] loc = new LocationWrapper[100];

        for(int i = 0; i < 100; ++i) {
            loc[i] = new LocationWrapper(new Point2D.Double((double)i, (double)i), (long)i);
        }

        this.locations = new ArrayList(Arrays.asList(loc));
        Collections.sort(this.locations);
        this.candidateAmount = this.locations.size() - 1;
        this.intervals = this.initIntervals();
        this.k = k;
        this.m = m;
        this.gamma = gamma;
    }

    public ArrayList<Tuple2<Double, Double>> process(Tuple2<Double, Double> input) {
        int maximum = 100;
        Double latitude = (Double)input._1;
        Double longitude = (Double)input._2;
        int count = 0;
        LocationWrapper[] locationWrappers = new LocationWrapper[100];

        for(int i = 0; i < 10; ++i) {
            for(int j = 0; j < 10; ++j) {
                Double lat = latitude - (double)(i - 5) * 1.0E-4;
                Double lon = longitude - (double)(j - 5) * 1.0E-4;
                locationWrappers[count] = new LocationWrapper(new Point2D.Double(lat, lon), (long)((int)(Math.random() * (double)maximum)));
                ++count;
            }
        }

        this.locations = new ArrayList(Arrays.asList(locationWrappers));
        List<Point2D.Double> generate = this.generate(49);
        ArrayList<Tuple2<Double, Double>> tuple2s = new ArrayList();
        Iterator var13 = generate.iterator();

        while(var13.hasNext()) {
            Point2D.Double asd = (Point2D.Double)var13.next();
            tuple2s.add(new Tuple2(asd.getX(), asd.getY()));
        }

        return tuple2s;
    }

    public List<Point2D.Double> generate(int realLocationIndex) {
        if (realLocationIndex >= 0 && realLocationIndex < this.locations.size()) {
            BetaDistribution distribution = this.computeBetaDistribution(realLocationIndex);
            int testIndex = 0;
            int[] result = null;
            double maxEntropy = Double.NEGATIVE_INFINITY;

            int[] elementIndices;
            int var11;
            int i;
            for(i = 0; i < this.m; ++i) {
                elementIndices = this.generateDummyElementIndices(realLocationIndex, distribution);
                elementIndices[0] = realLocationIndex;
                double entropy = this.computeEntropyOfCondProbs(elementIndices);
                if (entropy > maxEntropy) {
                    result = elementIndices;
                    maxEntropy = entropy;
                }

                ++testIndex;
            }

            List<Point2D.Double> resultLocations = new ArrayList();
            elementIndices = result;
            int var16 = result.length;

            for(var11 = 0; var11 < var16; ++var11) {
                i = elementIndices[var11];
                LocationWrapper location = this.locations.get(i);
                location.incrementAmount();
                resultLocations.add(location.getPoint());
            }

            this.reSort();
            return resultLocations;
        } else {
            System.out.println("Error");
            return null;
        }
    }

    private double computeEntropyOfCondProbs(int[] elementIndices) {
        double[] probs = new double[elementIndices.length];

        int real;
        for(int i = 0; i < elementIndices.length; ++i) {
            real = elementIndices[i];
            double prob = 1.0;
            BetaDistribution d = this.computeBetaDistribution(real);
            int[] var8 = elementIndices;
            int var9 = elementIndices.length;

            for(int var10 = 0; var10 < var9; ++var10) {
                int e = var8[var10];
                if (real != e) {
                    prob *= this.computeProbability(e, real, d);
                }
            }

            probs[i] = prob;
        }

        real = probs.length;
        return MathUtilForLocation.entropy(probs);
    }

    private double computeProbability(int elementIndex, int realLocationIndex, BetaDistribution distribution) {
        int intervalIndex = this.elementIndexToIntervalIndex(elementIndex, realLocationIndex);
        return this.getIntervalProbability(intervalIndex, distribution);
    }

    private BetaDistribution computeBetaDistribution(int realLocationIndex) {
        double mode = (Double)this.intervals.get(realLocationIndex);
        return MathUtilForLocation.computeBetaDistribution(mode, this.gamma);
    }

    private int[] generateDummyElementIndices(int realLocationIndex, BetaDistribution distribution) {
        Set<Integer> intervalIndices = new HashSet();

        do {
            double value = distribution.sample();
            intervalIndices.add(this.getIntervalIndex(value));
        } while(intervalIndices.size() < this.k - 1);

        int[] elementIndices = new int[this.k];
        int i = 1;

        for(Iterator var6 = intervalIndices.iterator(); var6.hasNext(); ++i) {
            int element = (Integer)var6.next();
            elementIndices[i] = this.intervalIndexToElementIndex(element, realLocationIndex);
        }

        return elementIndices;
    }

    private int getIntervalIndex(double value) {
        int intervalIndex = (int)(value * (double)this.candidateAmount);
        if (intervalIndex == this.candidateAmount) {
            intervalIndex = this.candidateAmount - 1;
        }

        return intervalIndex;
    }

    private double getIntervalProbability(int intervalIndex, BetaDistribution distribution) {
        double lower = (Double)this.intervals.get(intervalIndex);
        double upper = (Double)this.intervals.get(intervalIndex + 1);
        return distribution.probability(lower, upper);
    }

    private int intervalIndexToElementIndex(int intervalIndex, int realLocationIndex) {
        return intervalIndex >= realLocationIndex ? intervalIndex + 1 : intervalIndex;
    }

    private int elementIndexToIntervalIndex(int elementIndex, int realLocationIndex) {
        return elementIndex > realLocationIndex ? elementIndex - 1 : elementIndex;
    }

    private List<Double> initIntervals() {
        List<Double> intervals = new ArrayList();

        for(int i = 0; i < this.candidateAmount; ++i) {
            intervals.add((double)i / (double)this.candidateAmount);
        }

        intervals.add(1.0);
        return intervals;
    }

    private void reSort() {
        Collections.sort(this.locations);
    }

    public static double calcAlpha(double mode, double beta) {
        return MathUtilForLocation.calcAlpha(mode, beta);
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();
        LocationAnonymizer01 locAno = new LocationAnonymizer01(3, 15, 10.0);
        ArrayList<Tuple2<Double, Double>> process = locAno.process(new Tuple2(48.985774202242, 8.3942114974065));
        System.out.println(process);
        System.out.println();
        System.out.println("Time: " + (System.currentTimeMillis() - time));
    }
}

