package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * Represents an unordered pair of users.
 * The pair is always stored as: firstUser <= secondUser
 */
public class UserPairWritable
        implements WritableComparable<UserPairWritable> {

    private long firstUser;
    private long secondUser;

    /* Empty constructor required by Hadoop.*/
    public UserPairWritable() {
    }

    /**
     * Creates a pair using two user IDs.
     */
    public UserPairWritable(long userA, long userB) {
        set(userA, userB);
    }

    /**
     * Stores the two users in increasing order.
     */
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

    /**
     * Serializes the two user IDs.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(firstUser);
        out.writeLong(secondUser);
    }

    /**
     * Reads the two user IDs.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        firstUser = in.readLong();
        secondUser = in.readLong();
    }

    /**
     * Orders pairs first by the first user
     * and then by the second user.
     */
    @Override
    public int compareTo(UserPairWritable other) {
        int firstComparison =
                Long.compare(firstUser, other.firstUser);

        if (firstComparison != 0) {
            return firstComparison;
        }

        return Long.compare(
                secondUser,
                other.secondUser
        );
    }

    /**
     * Two pairs are equal if both users are equal.
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (!(object instanceof UserPairWritable)) {
            return false;
        }

        UserPairWritable other =
                (UserPairWritable) object;

        return firstUser == other.firstUser
                && secondUser == other.secondUser;
    }

    /**
     * Generates the hash code using both users.
     */
    @Override
    public int hashCode() {
        int result =
                (int) (firstUser ^ (firstUser >>> 32));

        result = 31 * result
                + (int) (secondUser
                ^ (secondUser >>> 32));

        return result;
    }

    /**
     * Returns the pair in text format.
     */
    @Override
    public String toString() {
        return firstUser + "," + secondUser;
    }
}