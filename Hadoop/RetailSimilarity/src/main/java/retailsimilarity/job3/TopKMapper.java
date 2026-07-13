package retailsimilarity.job3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import retailsimilarity.writable.SimilarUserWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Converts each canonical, unordered pair into two directed candidates:
 *
 * (user1, user2) -> score
 *
 * becomes:
 *
 * user1 -> (user2, score)
 * user2 -> (user1, score)
 */
public class TopKMapper extends Mapper<
        UserPairWritable,
        DoubleWritable,
        LongWritable,
        SimilarUserWritable> {

    public enum TopKMapCounters {
        DIRECTED_CANDIDATES,
        INVALID_SCORES,
        SELF_PAIRS
    }

    private final LongWritable outputUser = new LongWritable();
    private final SimilarUserWritable outputCandidate =
            new SimilarUserWritable();

    @Override
    protected void map(
            UserPairWritable key,
            DoubleWritable value,
            Context context
    ) throws IOException, InterruptedException {

        long firstUser = key.getFirstUser();
        long secondUser = key.getSecondUser();
        double score = value.get();

        if (firstUser == secondUser) {
            context.getCounter(TopKMapCounters.SELF_PAIRS).increment(1L);
            return;
        }
        if (!Double.isFinite(score)) {
            context.getCounter(TopKMapCounters.INVALID_SCORES).increment(1L);
            return;
        }

        emit(firstUser, secondUser, score, context);
        emit(secondUser, firstUser, score, context);
    }

    private void emit(
            long user,
            long similarUser,
            double score,
            Context context
    ) throws IOException, InterruptedException {
        outputUser.set(user);
        outputCandidate.set(similarUser, score);
        context.write(outputUser, outputCandidate);
        context.getCounter(TopKMapCounters.DIRECTED_CANDIDATES)
                .increment(1L);
    }
}
