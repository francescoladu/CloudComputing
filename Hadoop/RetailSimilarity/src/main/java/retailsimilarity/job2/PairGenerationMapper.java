package retailsimilarity.job2;

import java.io.IOException;
import java.util.SplittableRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

import retailsimilarity.RetailSimilarityConfig;
import retailsimilarity.writable.ItemBehaviorWritable;
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserListWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Hybrid candidate generation.
 *
 * Small posting lists are processed exactly.
 * Large posting lists are deterministically shuffled and converted into a
 * bounded-degree graph. Each user has at most k sampled neighbours, so the
 * number of emitted pairs is O(n * k), not O(n^2).
 */
public class PairGenerationMapper extends Mapper<
        ItemBehaviorWritable,
        UserListWritable,
        UserPairWritable,
        SimilarityWritable> {

    public enum PairCounters {
        EXACT_BUY_POSTING_LISTS,
        EXACT_PV_POSTING_LISTS,
        SAMPLED_BUY_POSTING_LISTS,
        SAMPLED_PV_POSTING_LISTS,
        GENERATED_BUY_PAIRS,
        GENERATED_PV_PAIRS,
        POTENTIAL_PAIRS_NOT_GENERATED,
        ODD_DEGREE_ADJUSTMENTS
    }

    private int exactMaxUsersBuy;
    private int exactMaxUsersPv;
    private int maxNeighboursBuy;
    private int maxNeighboursPv;
    private long samplingSeed;

    private boolean iufEnabled;
    private long totalUsersBuy;
    private long totalUsersPv;
    private double iufSmoothing;
    private double iufMaxWeight;

    private final UserPairWritable outputKey =
            new UserPairWritable();
    private final SimilarityWritable outputValue =
            new SimilarityWritable();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();

        exactMaxUsersBuy = configuration.getInt(
                RetailSimilarityConfig.EXACT_MAX_USERS_BUY,
                RetailSimilarityConfig.DEFAULT_EXACT_MAX_USERS_BUY
        );
        exactMaxUsersPv = configuration.getInt(
                RetailSimilarityConfig.EXACT_MAX_USERS_PV,
                RetailSimilarityConfig.DEFAULT_EXACT_MAX_USERS_PV
        );
        maxNeighboursBuy = configuration.getInt(
                RetailSimilarityConfig.MAX_NEIGHBOURS_BUY,
                RetailSimilarityConfig.DEFAULT_MAX_NEIGHBOURS_BUY
        );
        maxNeighboursPv = configuration.getInt(
                RetailSimilarityConfig.MAX_NEIGHBOURS_PV,
                RetailSimilarityConfig.DEFAULT_MAX_NEIGHBOURS_PV
        );
        samplingSeed = configuration.getLong(
                RetailSimilarityConfig.SAMPLING_SEED,
                RetailSimilarityConfig.DEFAULT_SAMPLING_SEED
        );

        iufEnabled = configuration.getBoolean(
                RetailSimilarityConfig.IUF_ENABLED,
                true
        );
        totalUsersBuy = configuration.getLong(
                RetailSimilarityConfig.TOTAL_USERS_BUY,
                -1L
        );
        totalUsersPv = configuration.getLong(
                RetailSimilarityConfig.TOTAL_USERS_PV,
                -1L
        );
        iufSmoothing = configuration.getDouble(
                RetailSimilarityConfig.IUF_SMOOTHING,
                RetailSimilarityConfig.DEFAULT_IUF_SMOOTHING
        );
        iufMaxWeight = configuration.getDouble(
                RetailSimilarityConfig.IUF_MAX_WEIGHT,
                RetailSimilarityConfig.DEFAULT_IUF_MAX_WEIGHT
        );

        if (iufEnabled) {
            if (totalUsersBuy <= 0L) {
                throw new IOException(
                        "IUF is enabled but "
                                + RetailSimilarityConfig.TOTAL_USERS_BUY
                                + " is not a positive number"
                );
            }
            if (totalUsersPv <= 0L) {
                throw new IOException(
                        "IUF is enabled but "
                                + RetailSimilarityConfig.TOTAL_USERS_PV
                                + " is not a positive number"
                );
            }
        }
        if (iufSmoothing < 0.0) {
            throw new IOException("IUF smoothing must be non-negative");
        }
        if (iufMaxWeight <= 0.0) {
            throw new IOException("IUF max weight must be positive");
        }
    }

    @Override
    protected void map(
            ItemBehaviorWritable key,
            UserListWritable value,
            Context context
    ) throws IOException, InterruptedException {

        long[] users = value.getUsers();
        int degree = users.length;
        if (degree < 2) {
            return;
        }

        double itemWeight = computeItemWeight(key.getBehavior(), degree);
        int exactLimit = exactLimitFor(key.getBehavior());

        if (exactLimit < 0 || degree <= exactLimit) {
            emitAllPairs(key.getBehavior(), users, itemWeight, context);
            incrementPostingListCounter(key.getBehavior(), true, context);
            return;
        }

        int requestedMaxNeighbours = maxNeighboursFor(key.getBehavior());
        emitBoundedPairs(
                key,
                users,
                requestedMaxNeighbours,
                itemWeight,
                context
        );
        incrementPostingListCounter(key.getBehavior(), false, context);
    }

    private void emitAllPairs(
            byte behavior,
            long[] users,
            double itemWeight,
            Context context
    ) throws IOException, InterruptedException {

        for (int first = 0; first < users.length - 1; first++) {
            for (int second = first + 1;
                 second < users.length;
                 second++) {
                emitPair(
                        behavior,
                        users[first],
                        users[second],
                        itemWeight,
                        context
                );
            }
        }
    }

    /**
     * Builds a deterministic near-regular graph over a shuffled posting list.
     * The configured value is the maximum total number of neighbours for a
     * user, not the number of forward neighbours.
     */
    private void emitBoundedPairs(
            ItemBehaviorWritable key,
            long[] originalUsers,
            int requestedMaxNeighbours,
            double itemWeight,
            Context context
    ) throws IOException, InterruptedException {

        int degree = originalUsers.length;
        int effectiveNeighbours = Math.min(
                Math.max(requestedMaxNeighbours, 0),
                degree - 1
        );

        long possiblePairs = numberOfPairs(degree);
        if (effectiveNeighbours == degree - 1) {
            emitAllPairs(
                    key.getBehavior(),
                    originalUsers,
                    itemWeight,
                    context
            );
            return;
        }

        if ((degree & 1) == 1 && (effectiveNeighbours & 1) == 1) {
            effectiveNeighbours--;
            context.getCounter(PairCounters.ODD_DEGREE_ADJUSTMENTS)
                    .increment(1L);
        }

        if (effectiveNeighbours <= 0) {
            context.getCounter(PairCounters.POTENTIAL_PAIRS_NOT_GENERATED)
                    .increment(possiblePairs);
            return;
        }

        long[] users = originalUsers.clone();
        long itemSeed = mix64(key.getItemId())
                ^ mix64(key.getBehavior())
                ^ samplingSeed;
        shuffle(users, new SplittableRandom(itemSeed));

        int symmetricOffsets = effectiveNeighbours / 2;
        long generatedPairs = 0L;

        // Each offset contributes two neighbours per user and n unique edges.
        for (int offset = 1; offset <= symmetricOffsets; offset++) {
            for (int first = 0; first < degree; first++) {
                int second = (first + offset) % degree;
                emitPair(
                        key.getBehavior(),
                        users[first],
                        users[second],
                        itemWeight,
                        context
                );
                generatedPairs++;
            }
        }

        // For even n, an antipodal matching adds one neighbour per user.
        if ((effectiveNeighbours & 1) == 1) {
            int half = degree / 2;
            for (int first = 0; first < half; first++) {
                int second = first + half;
                emitPair(
                        key.getBehavior(),
                        users[first],
                        users[second],
                        itemWeight,
                        context
                );
                generatedPairs++;
            }
        }

        context.getCounter(PairCounters.POTENTIAL_PAIRS_NOT_GENERATED)
                .increment(possiblePairs - generatedPairs);
    }

    private void emitPair(
            byte behavior,
            long firstUser,
            long secondUser,
            double itemWeight,
            Context context
    ) throws IOException, InterruptedException {

        outputKey.set(firstUser, secondUser);

        if (behavior == ItemBehaviorWritable.BUY) {
            outputValue.setBuyContribution(itemWeight);
            context.write(outputKey, outputValue);
            context.getCounter(PairCounters.GENERATED_BUY_PAIRS)
                    .increment(1L);
        } else if (behavior == ItemBehaviorWritable.PV) {
            outputValue.setPvContribution(itemWeight);
            context.write(outputKey, outputValue);
            context.getCounter(PairCounters.GENERATED_PV_PAIRS)
                    .increment(1L);
        }
    }

    private double computeItemWeight(byte behavior, int degree) {
        if (!iufEnabled) {
            return 1.0;
        }

        long totalUsersForBehavior = behavior == ItemBehaviorWritable.BUY
                ? totalUsersBuy
                : totalUsersPv;
        double numerator = totalUsersForBehavior + iufSmoothing;
        double denominator = degree + iufSmoothing;
        double weight = Math.log(numerator / denominator);

        if (!Double.isFinite(weight) || weight < 0.0) {
            weight = 0.0;
        }
        return Math.min(weight, iufMaxWeight);
    }

    private int exactLimitFor(byte behavior) {
        return behavior == ItemBehaviorWritable.BUY
                ? exactMaxUsersBuy
                : exactMaxUsersPv;
    }

    private int maxNeighboursFor(byte behavior) {
        return behavior == ItemBehaviorWritable.BUY
                ? maxNeighboursBuy
                : maxNeighboursPv;
    }

    private void incrementPostingListCounter(
            byte behavior,
            boolean exact,
            Context context
    ) {
        if (behavior == ItemBehaviorWritable.BUY) {
            context.getCounter(
                    exact
                            ? PairCounters.EXACT_BUY_POSTING_LISTS
                            : PairCounters.SAMPLED_BUY_POSTING_LISTS
            ).increment(1L);
        } else {
            context.getCounter(
                    exact
                            ? PairCounters.EXACT_PV_POSTING_LISTS
                            : PairCounters.SAMPLED_PV_POSTING_LISTS
            ).increment(1L);
        }
    }

    private static long numberOfPairs(int users) {
        return ((long) users * (users - 1L)) / 2L;
    }

    private static void shuffle(long[] values, SplittableRandom random) {
        for (int index = values.length - 1; index > 0; index--) {
            int other = random.nextInt(index + 1);
            long temporary = values[index];
            values[index] = values[other];
            values[other] = temporary;
        }
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30))
                * 0xBF58476D1CE4E5B9L;
        value = (value ^ (value >>> 27))
                * 0x94D049BB133111EBL;
        return value ^ (value >>> 31);
    }
}
