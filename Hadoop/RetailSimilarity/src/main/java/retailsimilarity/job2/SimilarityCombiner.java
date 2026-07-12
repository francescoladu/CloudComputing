package retailsimilarity.job2;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Mapper-local aggregation for equal user pairs.
 */
public class SimilarityCombiner extends Reducer<
        UserPairWritable,
        SimilarityWritable,
        UserPairWritable,
        SimilarityWritable> {

    private final SimilarityWritable outputValue =
            new SimilarityWritable();

    @Override
    protected void reduce(
            UserPairWritable key,
            Iterable<SimilarityWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        long commonBuy = 0L;
        long commonPv = 0L;
        double weightedBuy = 0.0;
        double weightedPv = 0.0;

        for (SimilarityWritable value : values) {
            commonBuy += value.getCommonBuyItems();
            commonPv += value.getCommonPvItems();
            weightedBuy += value.getWeightedBuy();
            weightedPv += value.getWeightedPv();
        }

        outputValue.set(
                commonBuy,
                commonPv,
                weightedBuy,
                weightedPv,
                0.0
        );
        context.write(key, outputValue);
    }
}
