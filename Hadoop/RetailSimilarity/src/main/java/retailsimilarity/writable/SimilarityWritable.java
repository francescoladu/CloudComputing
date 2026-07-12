package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Similarity accumulator and final result for one user pair.
 *
 * Mapper/combiner fields:
 * - commonBuyItems/commonPvItems: raw shared-item contributions;
 * - weightedBuy/weightedPv: sum of IUF weights.
 *
 * Reducer field:
 * - score: behavior-weighted final score.
 */
public class SimilarityWritable implements Writable {

    private long commonBuyItems;
    private long commonPvItems;
    private double weightedBuy;
    private double weightedPv;
    private double score;

    public SimilarityWritable() {
    }

    public SimilarityWritable(
            long commonBuyItems,
            long commonPvItems,
            double weightedBuy,
            double weightedPv,
            double score
    ) {
        set(
                commonBuyItems,
                commonPvItems,
                weightedBuy,
                weightedPv,
                score
        );
    }

    public void set(
            long commonBuyItems,
            long commonPvItems,
            double weightedBuy,
            double weightedPv,
            double score
    ) {
        this.commonBuyItems = commonBuyItems;
        this.commonPvItems = commonPvItems;
        this.weightedBuy = weightedBuy;
        this.weightedPv = weightedPv;
        this.score = score;
    }

    public void setBuyContribution(double itemWeight) {
        set(1L, 0L, itemWeight, 0.0, 0.0);
    }

    public void setPvContribution(double itemWeight) {
        set(0L, 1L, 0.0, itemWeight, 0.0);
    }

    public long getCommonBuyItems() {
        return commonBuyItems;
    }

    public long getCommonPvItems() {
        return commonPvItems;
    }

    public double getWeightedBuy() {
        return weightedBuy;
    }

    public double getWeightedPv() {
        return weightedPv;
    }

    public double getScore() {
        return score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(commonBuyItems);
        out.writeLong(commonPvItems);
        out.writeDouble(weightedBuy);
        out.writeDouble(weightedPv);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        commonBuyItems = in.readLong();
        commonPvItems = in.readLong();
        weightedBuy = in.readDouble();
        weightedPv = in.readDouble();
        score = in.readDouble();
    }

    /**
     * Final TextOutputFormat representation:
     * commonBuy,commonPv,weightedBuy,weightedPv,score
     */
    @Override
    public String toString() {
        return commonBuyItems
                + "," + commonPvItems
                + "," + weightedBuy
                + "," + weightedPv
                + "," + score;
    }
}
