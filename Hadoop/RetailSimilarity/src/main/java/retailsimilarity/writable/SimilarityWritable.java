package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Contiene le due similarità:
 *
 * buyCount = prodotti acquistati in comune
 * favCount = prodotti preferiti in comune
 */
public class SimilarityWritable implements Writable {

    private long buyCount;
    private long favCount;

    public SimilarityWritable() {
    }

    public SimilarityWritable(long buyCount, long favCount) {
        set(buyCount, favCount);
    }

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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(buyCount);
        out.writeLong(favCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        buyCount = in.readLong();
        favCount = in.readLong();
    }

    @Override
    public String toString() {
        return buyCount + "," + favCount;
    }
}