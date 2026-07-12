package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

/**
 * Sorted array of distinct users associated with an item and behavior.
 */
public class UserListWritable implements Writable {

    private long[] users;

    public UserListWritable() {
        users = new long[0];
    }

    public UserListWritable(Collection<Long> users) {
        setUsers(users);
    }

    public void setUsers(Collection<Long> userCollection) {
        users = new long[userCollection.size()];
        int index = 0;
        for (Long user : userCollection) {
            if (user == null) {
                throw new IllegalArgumentException("User collection contains null");
            }
            users[index++] = user;
        }
        Arrays.sort(users);
    }

    public void setUsers(long[] userArray) {
        users = userArray.clone();
        Arrays.sort(users);
    }

    /**
     * The returned array must be treated as read-only.
     */
    public long[] getUsers() {
        return users;
    }

    public int size() {
        return users.length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(users.length);
        for (long user : users) {
            out.writeLong(user);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IOException("Invalid user list size: " + size);
        }
        users = new long[size];
        for (int index = 0; index < size; index++) {
            users[index] = in.readLong();
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(users);
    }
}
