package retailsimilarity.job2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.RetailSimilarityConfig;
import retailsimilarity.writable.UserPairWritable;

/**
 * Aggregates all similarity contributions for a candidate user pair.
 */
public class SimilarityReducer extends Reducer<
        UserPairWritable,
        DoubleWritable,
        UserPairWritable,
        DoubleWritable> {

    public enum SimilarityCounters {
        EMITTED_PAIRS,
        FILTERED_BY_SCORE
    }

    private double minimumScore;

    private final DoubleWritable outputValue =
            new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        minimumScore = configuration.getDouble(
                RetailSimilarityConfig.MIN_SCORE,
                RetailSimilarityConfig.DEFAULT_MIN_SCORE
        );

        if (minimumScore < 0.0) {
            throw new IOException("Minimum score cannot be negative");
        }
    }

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

        if (totalContribution < minimumScore) {
            context.getCounter(SimilarityCounters.FILTERED_BY_SCORE)
                    .increment(1L);
            return;
        }

        outputValue.set(totalContribution);
        context.write(key, outputValue);
        context.getCounter(SimilarityCounters.EMITTED_PAIRS)
                .increment(1L);
    }
}
