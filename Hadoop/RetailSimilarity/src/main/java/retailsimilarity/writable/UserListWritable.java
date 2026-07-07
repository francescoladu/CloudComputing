package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.Writable;

/**
 * Contains the ordered list of distinct users
 * related to a specific item and behavior.
 */
public class UserListWritable implements Writable {

    private long[] users;

    /**
     * Empty constructor required by Hadoop.
     */
    public UserListWritable() {
        users = new long[0];
    }

    /**
     * Creates the user list from a collection.
     */
    public UserListWritable(Collection<Long> users) {
        setUsers(users);
    }

    /**
     * Copies the users into an array and sorts them.
     */
    public void setUsers(Collection<Long> userCollection) {
        users = new long[userCollection.size()];

        int index = 0;

        for (Long user : userCollection) {
            users[index] = user;
            index++;
        }

        Arrays.sort(users);
    }

    public long[] getUsers() {
        return users;
    }

    public int size() {
        return users.length;
    }

    /**
     * Serializes the list size and all user IDs.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(users.length);

        for (long user : users) {
            out.writeLong(user);
        }
    }

    /**
     * Reads the list size and all user IDs.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();

        if (size < 0) {
            throw new IOException(
                    "Invalid user list size: " + size
            );
        }

        users = new long[size];

        for (int index = 0; index < size; index++) {
            users[index] = in.readLong();
        }
    }

    /**
     * Returns the user list in text format.
     */
    @Override
    public String toString() {
        return Arrays.toString(users);
    }
}
