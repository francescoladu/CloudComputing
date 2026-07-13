package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Ordered Top-K list for one user.
 */
public class SimilarUserListWritable implements Writable {

    private long[] userIds = new long[0];
    private double[] scores = new double[0];

    public SimilarUserListWritable() {
    }

    public void setUsers(List<SimilarUserWritable> users) {
        userIds = new long[users.size()];
        scores = new double[users.size()];

        for (int index = 0; index < users.size(); index++) {
            SimilarUserWritable user = users.get(index);
            userIds[index] = user.getUserId();
            scores[index] = user.getScore();
        }
    }

    public int size() {
        return userIds.length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userIds.length);
        for (int index = 0; index < userIds.length; index++) {
            out.writeLong(userIds[index]);
            out.writeDouble(scores[index]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IOException("Invalid similar-user list size: " + size);
        }

        userIds = new long[size];
        scores = new double[size];
        for (int index = 0; index < size; index++) {
            userIds[index] = in.readLong();
            scores[index] = in.readDouble();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        for (int index = 0; index < userIds.length; index++) {
            if (index > 0) {
                builder.append(", ");
            }
            builder.append('(')
                    .append(userIds[index])
                    .append(',')
                    .append(scores[index])
                    .append(')');
        }
        builder.append(']');
        return builder.toString();
    }
}
