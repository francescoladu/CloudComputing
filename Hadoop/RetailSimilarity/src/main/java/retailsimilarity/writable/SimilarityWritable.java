package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Contains the two similarity values
 * calculated for a pair of users.

 * buyCount = common purchased items
 * pvCount = common pvourite items
 */
public class SimilarityWritable implements Writable {

    private long buyCount;
    private long pvCount;

    /**
     * Empty constructor required by Hadoop.
     */
    public SimilarityWritable() {
    }

    /**
     * Creates a similarity value with both counts.
     */
    public SimilarityWritable(long buyCount, long pvCount) {
        set(buyCount, pvCount);
    }

    /**
     * Updates both similarity counts.
     */
    public void set(long buyCount, long pvCount) {
        this.buyCount = buyCount;
        this.pvCount = pvCount;
    }

    public long getBuyCount() {
        return buyCount;
    }

    public long getPvCount() {
        return pvCount;
    }

    /**
     * Serializes the two similarity counts.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(buyCount);
        out.writeLong(pvCount);
    }

    /**
     * Reads the two similarity counts.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        buyCount = in.readLong();
        pvCount = in.readLong();
    }

    /**
     * Returns the similarity values in text format.
     */
    @Override
    public String toString() {
        return buyCount + "," + pvCount;
    }
}

