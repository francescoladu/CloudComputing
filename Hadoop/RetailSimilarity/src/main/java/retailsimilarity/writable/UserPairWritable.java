package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Canonical unordered pair. The smaller user ID is always stored first.
 */
public class UserPairWritable
        implements WritableComparable<UserPairWritable> {

    private long firstUser;
    private long secondUser;

    public UserPairWritable() {
    }

    public UserPairWritable(long userA, long userB) {
        set(userA, userB);
    }

    public void set(long userA, long userB) {
        if (userA <= userB) {
            firstUser = userA;
            secondUser = userB;
        } else {
            firstUser = userB;
            secondUser = userA;
        }
    }

    public long getFirstUser() {
        return firstUser;
    }

    public long getSecondUser() {
        return secondUser;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(firstUser);
        out.writeLong(secondUser);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstUser = in.readLong();
        secondUser = in.readLong();
    }

    @Override
    public int compareTo(UserPairWritable other) {
        int firstComparison = Long.compare(firstUser, other.firstUser);
        if (firstComparison != 0) {
            return firstComparison;
        }
        return Long.compare(secondUser, other.secondUser);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UserPairWritable)) {
            return false;
        }
        UserPairWritable other = (UserPairWritable) object;
        return firstUser == other.firstUser
                && secondUser == other.secondUser;
    }

    @Override
    public int hashCode() {
        int result = (int) (firstUser ^ (firstUser >>> 32));
        result = 31 * result
                + (int) (secondUser ^ (secondUser >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return firstUser + "," + secondUser;
    }
}
