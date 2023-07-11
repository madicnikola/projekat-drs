package domain;

public class AvgScore {
    private Double avgSum;
    private int counter;

    public AvgScore(Double avgSum, int counter) {
        this.avgSum = avgSum;
        this.counter = counter;
    }

    public Double getAvgForDirector() {
        if (counter != 0) return avgSum / counter;
        return 0.0;
    }

    public Double getAvgSum() {
        return avgSum;
    }

    public int getCounter() {
        return counter;
    }


    @Override
    public String toString() {
        return "AvgScore{" +
                "avgSum=" + avgSum +
                ", counter=" + counter +
                '}';
    }
}