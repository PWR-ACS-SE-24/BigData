import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

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

    public void setup(Configuration conf, String inputPath) throws IOException {
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(this.splitMb * 1024 * 1024));
        FileSystem fs = FileSystem.get(conf);
        fs.setReplication(new Path(inputPath), this.replication);
    }

    public void teardown(Configuration conf, String inputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.setReplication(new Path(inputPath), (short) 3);
    }

    public static final BenchmarkConfig DEFAULT = new BenchmarkConfig(1, (short) 3, 128);
}
