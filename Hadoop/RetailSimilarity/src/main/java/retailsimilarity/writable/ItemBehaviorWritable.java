package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Key used in the first MapReduce job.
 *
 * It is composed by: (itemId, behavior)
 * behavior:
 * 1 = buy
 * 2 = fav
 */
public class ItemBehaviorWritable
        implements WritableComparable<ItemBehaviorWritable> {

    public static final byte BUY = 1;
    public static final byte FAV = 2;

    private long itemId;
    private byte behavior;

    /**
     * Empty constructor required by Hadoop.
     */
    public ItemBehaviorWritable() {
    }

    /**
     * Creates a key with item ID and behavior.
     */
    public ItemBehaviorWritable(long itemId, byte behavior) {
        set(itemId, behavior);
    }

    /**
     * Updates both fields of the key.
     */
    public void set(long itemId, byte behavior) {
        this.itemId = itemId;
        this.behavior = behavior;
    }

    public long getItemId() {
        return itemId;
    }

    public byte getBehavior() {
        return behavior;
    }

    /**
     * Returns the textual name of the behavior.
     */
    public String getBehaviorName() {
        if (behavior == BUY) {
            return "buy";
        }

        if (behavior == FAV) {
            return "fav";
        }

        return "unknown";
    }

    /**
     * Serializes item ID and behavior.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeByte(behavior);
    }

    /**
     * Reads item ID and behavior.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        itemId = in.readLong();
        behavior = in.readByte();
    }

    /**
     * Orders keys first by item ID and then by behavior.
     */
    @Override
    public int compareTo(ItemBehaviorWritable other) {
        int itemComparison =
                Long.compare(itemId, other.itemId);

        if (itemComparison != 0) {
            return itemComparison;
        }

        return Byte.compare(behavior, other.behavior);
    }

    /**
     * Two keys are equal if both fields are equal.
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (!(object instanceof ItemBehaviorWritable)) {
            return false;
        }

        ItemBehaviorWritable other =
                (ItemBehaviorWritable) object;

        return itemId == other.itemId
                && behavior == other.behavior;
    }

    /**
     * Generates the hash code using both fields.
     */
    @Override
    public int hashCode() {
        int result =
                (int) (itemId ^ (itemId >>> 32));

        result = 31 * result + behavior;

        return result;
    }

    /**
     * Returns the key in text format.
     */
    @Override
    public String toString() {
        return itemId + "," + getBehaviorName();
    
}   
        }