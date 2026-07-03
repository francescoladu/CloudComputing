package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Chiave del primo job:
 *
 * (itemId, behavior)
 *
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

    public ItemBehaviorWritable() {
    }

    public ItemBehaviorWritable(long itemId, byte behavior) {
        set(itemId, behavior);
    }

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

    public String getBehaviorName() {
        if (behavior == BUY) {
            return "buy";
        }

        if (behavior == FAV) {
            return "fav";
        }

        return "unknown";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeByte(behavior);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        itemId = in.readLong();
        behavior = in.readByte();
    }

    @Override
    public int compareTo(ItemBehaviorWritable other) {
        int itemComparison = Long.compare(itemId, other.itemId);

        if (itemComparison != 0) {
            return itemComparison;
        }

        return Byte.compare(behavior, other.behavior);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (!(object instanceof ItemBehaviorWritable)) {
            return false;
        }

        ItemBehaviorWritable other = (ItemBehaviorWritable) object;

        return itemId == other.itemId
                && behavior == other.behavior;
    }

    @Override
    public int hashCode() {
        int result = (int) (itemId ^ (itemId >>> 32));
        result = 31 * result + behavior;
        return result;
    }

    @Override
    public String toString() {
        return itemId + "," + getBehaviorName();
    }
}
