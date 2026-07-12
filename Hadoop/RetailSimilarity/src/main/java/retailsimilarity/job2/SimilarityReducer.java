package retailsimilarity.job2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.RetailSimilarityConfig;
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Aggregates all contributions for a candidate user pair and computes:
 *
 * score = buyBehaviorWeight * weightedBuy
 *       + pvBehaviorWeight  * weightedPv
 */
public class SimilarityReducer extends Reducer<
        UserPairWritable,
        SimilarityWritable,
        UserPairWritable,
        SimilarityWritable> {

    public enum SimilarityCounters {
        EMITTED_PAIRS,
        FILTERED_BY_SUPPORT,
        FILTERED_BY_SCORE
    }

    private long minimumBuyCount;
    private long minimumPvCount;
    private double minimumScore;
    private double buyBehaviorWeight;
    private double pvBehaviorWeight;

    private final SimilarityWritable outputValue =
            new SimilarityWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();

        minimumBuyCount = configuration.getLong(
                RetailSimilarityConfig.MIN_BUY_COUNT,
                0L
        );
        minimumPvCount = configuration.getLong(
                RetailSimilarityConfig.MIN_PV_COUNT,
                0L
        );
        minimumScore = configuration.getDouble(
                RetailSimilarityConfig.MIN_SCORE,
                0.0
        );
        buyBehaviorWeight = configuration.getDouble(
                RetailSimilarityConfig.BUY_BEHAVIOR_WEIGHT,
                RetailSimilarityConfig.DEFAULT_BUY_BEHAVIOR_WEIGHT
        );
        pvBehaviorWeight = configuration.getDouble(
                RetailSimilarityConfig.PV_BEHAVIOR_WEIGHT,
                RetailSimilarityConfig.DEFAULT_PV_BEHAVIOR_WEIGHT
        );

        if (minimumBuyCount < 0L || minimumPvCount < 0L) {
            throw new IOException("Minimum support counts cannot be negative");
        }
        if (minimumScore < 0.0) {
            throw new IOException("Minimum score cannot be negative");
        }
        if (buyBehaviorWeight < 0.0 || pvBehaviorWeight < 0.0) {
            throw new IOException("Behavior weights cannot be negative");
        }
    }

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

        // The pair survives if it reaches either configured support threshold.
        if (commonBuy < minimumBuyCount
                && commonPv < minimumPvCount) {
            context.getCounter(SimilarityCounters.FILTERED_BY_SUPPORT)
                    .increment(1L);
            return;
        }

        double score = buyBehaviorWeight * weightedBuy
                + pvBehaviorWeight * weightedPv;

        if (score < minimumScore) {
            context.getCounter(SimilarityCounters.FILTERED_BY_SCORE)
                    .increment(1L);
            return;
        }

        outputValue.set(
                commonBuy,
                commonPv,
                weightedBuy,
                weightedPv,
                score
        );
        context.write(key, outputValue);
        context.getCounter(SimilarityCounters.EMITTED_PAIRS)
                .increment(1L);
    }
}
