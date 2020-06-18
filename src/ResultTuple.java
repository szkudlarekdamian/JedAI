public class ResultTuple {
    public double fMeasure;
    public double precision;
    public double recall;

    public ResultTuple(double fMeasure, double precision, double recall) {
        this.fMeasure = fMeasure;
        this.precision = precision;
        this.recall = recall;
    }
}
