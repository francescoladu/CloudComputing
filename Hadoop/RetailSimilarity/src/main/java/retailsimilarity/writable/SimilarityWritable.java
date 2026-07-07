package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Contains the two similarity values
 * calculated for a pair of users.

 * buyCount = common purchased items
 * favCount = common favourite items
 */
public class SimilarityWritable implements Writable {

    private long buyCount;
    private long favCount;

    /**
     * Empty constructor required by Hadoop.
     */
    public SimilarityWritable() {
    }

    /**
     * Creates a similarity value with both counts.
     */
    public SimilarityWritable(long buyCount, long favCount) {
        set(buyCount, favCount);
    }

    /**
     * Updates both similarity counts.
     */
    public void set(long buyCount, long favCount) {
        this.buyCount = buyCount;
        this.favCount = favCount;
    }

    public long getBuyCount() {
        return buyCount;
    }

    public long getFavCount() {
        return favCount;
    }

    /**
     * Serializes the two similarity counts.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(buyCount);
        out.writeLong(favCount);
    }

    /**
     * Reads the two similarity counts.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        buyCount = in.readLong();
        favCount = in.readLong();
    }

    /**
     * Returns the similarity values in text format.
     */
    @Override
    public String toString() {
        return buyCount + "," + favCount;
    }
}

