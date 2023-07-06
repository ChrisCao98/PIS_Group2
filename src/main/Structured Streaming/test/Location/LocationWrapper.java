package test.Location;



import java.awt.geom.Point2D;

public class LocationWrapper implements Comparable<LocationWrapper> {
    private final Point2D.Double point;
    private long amount;

    public LocationWrapper(Point2D.Double point, long amount) {
        this.point = point;
        this.amount = amount;
    }

    public LocationWrapper(Point2D.Double point) {
        this(point, 0L);
    }

    public void incrementAmount() {
        ++this.amount;
    }

    public long getAmount() {
        return this.amount;
    }

    public void setAmount(int amount) {
        this.amount = (long)amount;
    }

    public Point2D.Double getPoint() {
        return this.point;
    }

    public int compareTo(LocationWrapper other) {
        if (this.amount == other.amount) {
            return 0;
        } else {
            return this.amount < other.amount ? -1 : 1;
        }
    }

    public String toString() {
        return "[" + this.point + " | " + this.amount + "]";
    }
}

