public class BenchmarkConfig {
    public int reducers = 1;
    public short replication = 1;
    public int splitMb = 1;

    public BenchmarkConfig(int reducers, short replication, int splitMb) {
        this.reducers = reducers;
        this.replication = replication;
        this.splitMb = splitMb;
    }

    @Override
    public String toString() {
        return "BenchmarkConfig{" +
                "reducers=" + reducers +
                ", replication=" + replication +
                ", splitMb=" + splitMb +
                '}';
    }

    public static final BenchmarkConfig DEFAULT = new BenchmarkConfig(1, (short) 3, 128);
}
