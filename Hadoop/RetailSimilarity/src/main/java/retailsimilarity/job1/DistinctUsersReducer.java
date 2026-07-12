package retailsimilarity.job1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.RetailSimilarityConfig;
import retailsimilarity.writable.ItemBehaviorWritable;
import retailsimilarity.writable.UserListWritable;

/**
 * Input:  (itemId, behavior) -> [userId, ...]
 * Output: (itemId, behavior) -> [sorted distinct users]
 */
public class DistinctUsersReducer extends Reducer<
        ItemBehaviorWritable,
        LongWritable,
        ItemBehaviorWritable,
        UserListWritable> {

    public enum ItemCounters {
        EMITTED_BUY_POSTING_LISTS,
        EMITTED_PV_POSTING_LISTS,
        ITEMS_WITH_LESS_THAN_TWO_USERS,
        ITEMS_ABOVE_HARD_MAX_USERS,
        DISTINCT_BUY_EDGES,
        DISTINCT_PV_EDGES,
        POTENTIAL_BUY_ALL_PAIRS,
        POTENTIAL_PV_ALL_PAIRS,
        PLANNED_BUY_PAIR_CONTRIBUTIONS,
        PLANNED_PV_PAIR_CONTRIBUTIONS
    }

    private int hardMaxUsersPerItem;
    private int exactMaxUsersBuy;
    private int exactMaxUsersPv;
    private int maxNeighboursBuy;
    private int maxNeighboursPv;

    private final UserListWritable outputValue =
            new UserListWritable();

    @Override
    protected void setup(Context context) {
        Configuration configuration = context.getConfiguration();

        hardMaxUsersPerItem = configuration.getInt(
                RetailSimilarityConfig.MAX_USERS_PER_ITEM,
                -1
        );
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
    }

    @Override
    protected void reduce(
            ItemBehaviorWritable key,
            Iterable<LongWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        Set<Long> distinctUsers = new HashSet<>();
        boolean aboveHardMaximum = false;

        for (LongWritable value : values) {
            if (aboveHardMaximum) {
                continue;
            }

            distinctUsers.add(value.get());
            if (hardMaxUsersPerItem > 0
                    && distinctUsers.size() > hardMaxUsersPerItem) {
                aboveHardMaximum = true;
                distinctUsers.clear();
            }
        }

        if (aboveHardMaximum) {
            context.getCounter(ItemCounters.ITEMS_ABOVE_HARD_MAX_USERS)
                    .increment(1L);
            return;
        }

        int degree = distinctUsers.size();
        if (degree < 2) {
            context.getCounter(ItemCounters.ITEMS_WITH_LESS_THAN_TWO_USERS)
                    .increment(1L);
            return;
        }

        long allPairs = numberOfPairs(degree);
        long plannedPairs = plannedPairs(
                degree,
                exactLimitFor(key.getBehavior()),
                maxNeighboursFor(key.getBehavior())
        );

        if (key.getBehavior() == ItemBehaviorWritable.BUY) {
            context.getCounter(ItemCounters.EMITTED_BUY_POSTING_LISTS)
                    .increment(1L);
            context.getCounter(ItemCounters.DISTINCT_BUY_EDGES)
                    .increment(degree);
            context.getCounter(ItemCounters.POTENTIAL_BUY_ALL_PAIRS)
                    .increment(allPairs);
            context.getCounter(ItemCounters.PLANNED_BUY_PAIR_CONTRIBUTIONS)
                    .increment(plannedPairs);
        } else {
            context.getCounter(ItemCounters.EMITTED_PV_POSTING_LISTS)
                    .increment(1L);
            context.getCounter(ItemCounters.DISTINCT_PV_EDGES)
                    .increment(degree);
            context.getCounter(ItemCounters.POTENTIAL_PV_ALL_PAIRS)
                    .increment(allPairs);
            context.getCounter(ItemCounters.PLANNED_PV_PAIR_CONTRIBUTIONS)
                    .increment(plannedPairs);
        }

        outputValue.setUsers(distinctUsers);
        context.write(key, outputValue);
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

    private static long plannedPairs(
            int degree,
            int exactLimit,
            int requestedMaxNeighbours
    ) {
        long allPairs = numberOfPairs(degree);
        if (exactLimit < 0 || degree <= exactLimit) {
            return allPairs;
        }

        int effectiveNeighbours = Math.min(
                Math.max(requestedMaxNeighbours, 0),
                degree - 1
        );

        if (effectiveNeighbours == degree - 1) {
            return allPairs;
        }

        // A regular graph with odd degree cannot exist on an odd number
        // of vertices. We reduce the degree by one in that case.
        if ((degree & 1) == 1 && (effectiveNeighbours & 1) == 1) {
            effectiveNeighbours--;
        }

        return ((long) degree * effectiveNeighbours) / 2L;
    }

    private static long numberOfPairs(int degree) {
        return ((long) degree * (degree - 1L)) / 2L;
    }
}
