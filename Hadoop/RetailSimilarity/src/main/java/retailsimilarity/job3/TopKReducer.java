package retailsimilarity.job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.RetailSimilarityConfig;
import retailsimilarity.writable.SimilarUserListWritable;
import retailsimilarity.writable.SimilarUserWritable;

/**
 * Keeps only the best K candidates for each user.
 *
 * The queue is a min-heap whose head is the worst retained candidate. This
 * makes every insertion O(log K) and bounds reducer memory to O(K).
 *
 * Ranking order:
 * 1. higher score first;
 * 2. for equal scores, smaller user ID first.
 */
public class TopKReducer extends Reducer<
        LongWritable,
        SimilarUserWritable,
        LongWritable,
        SimilarUserListWritable> {

    public enum TopKReduceCounters {
        USERS_EMITTED,
        CANDIDATES_SEEN,
        CANDIDATES_REJECTED,
        CANDIDATES_REPLACED,
        USERS_WITH_FEWER_THAN_K
    }

    private static final Comparator<SimilarUserWritable> WORST_FIRST =
            new Comparator<SimilarUserWritable>() {
                @Override
                public int compare(
                        SimilarUserWritable left,
                        SimilarUserWritable right
                ) {
                    int scoreComparison = Double.compare(
                            left.getScore(),
                            right.getScore()
                    );
                    if (scoreComparison != 0) {
                        return scoreComparison;
                    }

                    // At equal score, the larger user ID is the worse result
                    // and therefore must be closer to the head of the heap.
                    return Long.compare(
                            right.getUserId(),
                            left.getUserId()
                    );
                }
            };

    private static final Comparator<SimilarUserWritable> BEST_FIRST =
            new Comparator<SimilarUserWritable>() {
                @Override
                public int compare(
                        SimilarUserWritable left,
                        SimilarUserWritable right
                ) {
                    int scoreComparison = Double.compare(
                            right.getScore(),
                            left.getScore()
                    );
                    if (scoreComparison != 0) {
                        return scoreComparison;
                    }
                    return Long.compare(
                            left.getUserId(),
                            right.getUserId()
                    );
                }
            };

    private int topK;
    private final SimilarUserListWritable outputValue =
            new SimilarUserListWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        topK = configuration.getInt(
                RetailSimilarityConfig.OUTPUT_TOP_K,
                RetailSimilarityConfig.DEFAULT_OUTPUT_TOP_K
        );
        if (topK <= 0) {
            throw new IOException(
                    RetailSimilarityConfig.OUTPUT_TOP_K
                            + " must be greater than zero"
            );
        }
    }

    @Override
    protected void reduce(
            LongWritable user,
            Iterable<SimilarUserWritable> candidates,
            Context context
    ) throws IOException, InterruptedException {

        PriorityQueue<SimilarUserWritable> bestCandidates =
                new PriorityQueue<SimilarUserWritable>(topK, WORST_FIRST);

        for (SimilarUserWritable candidate : candidates) {
            context.getCounter(TopKReduceCounters.CANDIDATES_SEEN)
                    .increment(1L);

            SimilarUserWritable copy = new SimilarUserWritable(
                    candidate.getUserId(),
                    candidate.getScore()
            );

            if (bestCandidates.size() < topK) {
                bestCandidates.offer(copy);
                continue;
            }

            SimilarUserWritable worstRetained = bestCandidates.peek();
            if (isBetter(copy, worstRetained)) {
                bestCandidates.poll();
                bestCandidates.offer(copy);
                context.getCounter(TopKReduceCounters.CANDIDATES_REPLACED)
                        .increment(1L);
            } else {
                context.getCounter(TopKReduceCounters.CANDIDATES_REJECTED)
                        .increment(1L);
            }
        }

        if (bestCandidates.size() < topK) {
            context.getCounter(TopKReduceCounters.USERS_WITH_FEWER_THAN_K)
                    .increment(1L);
        }

        List<SimilarUserWritable> sortedCandidates =
                new ArrayList<SimilarUserWritable>(bestCandidates);
        sortedCandidates.sort(BEST_FIRST);

        outputValue.setUsers(sortedCandidates);
        context.write(user, outputValue);
        context.getCounter(TopKReduceCounters.USERS_EMITTED)
                .increment(1L);
    }

    private static boolean isBetter(
            SimilarUserWritable candidate,
            SimilarUserWritable retained
    ) {
        int scoreComparison = Double.compare(
                candidate.getScore(),
                retained.getScore()
        );
        if (scoreComparison != 0) {
            return scoreComparison > 0;
        }
        return candidate.getUserId() < retained.getUserId();
    }
}
