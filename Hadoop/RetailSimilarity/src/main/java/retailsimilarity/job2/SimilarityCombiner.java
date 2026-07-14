package retailsimilarity.job2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.writable.UserPairWritable;

/**
 * Mapper-local aggregation for equal user pairs.
 */
public class SimilarityCombiner extends Reducer<
        UserPairWritable,
        DoubleWritable,
        UserPairWritable,
        DoubleWritable> {

    private final DoubleWritable outputValue =
            new DoubleWritable();

    @Override
    protected void reduce(
            UserPairWritable key,
            Iterable<DoubleWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        double totalContribution = 0.0;
        for (DoubleWritable value : values) {
            totalContribution += value.get();
        }

        outputValue.set(totalContribution);
        context.write(key, outputValue);
    }
}
