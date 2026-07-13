package retailsimilarity.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Rappresenta un utente simile e il relativo punteggio.
 */
public class SimilarUserWritable implements Writable {

    private long userId;
    private double score;

    /**
     * Costruttore vuoto richiesto da Hadoop.
     */
    public SimilarUserWritable() {
    }

    public SimilarUserWritable(long userId, double score) {
        set(userId, score);
    }

    public void set(long userId, double score) {
        this.userId = userId;
        this.score = score;
    }

    public long getUserId() {
        return userId;
    }

    public double getScore() {
        return score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(userId);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readLong();
        score = in.readDouble();
    }

    @Override
    public String toString() {
        return "(" + userId + "," + score + ")";
    }
}