package retailsimilarity.job1;
 
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
 
import retailsimilarity.writable.ItemBehaviorWritable;
import retailsimilarity.writable.UserListWritable;
 
/**
* Removes duplicate users for each item and behavior.
*
* Input:  (itemId, behavior) -> [userId, ...]
* Output: (itemId, behavior) -> [distinct users]
*/
public class DistinctUsersReducer extends Reducer<
        ItemBehaviorWritable,
        LongWritable,
        ItemBehaviorWritable,
        UserListWritable> {
 
    // Counters used to check the reducer output.
    public enum ItemCounters {
        EMITTED_ITEMS,
        ITEMS_WITH_LESS_THAN_TWO_USERS,
        ITEMS_ABOVE_MAX_USERS
    }
 
    private int maxUsersPerItem;
 
    // Reuse the same object for every reducer output.
    private final UserListWritable outputValue =
            new UserListWritable();
 
    /**
     * Reads the maximum number of users allowed for one item.
     */
    
    @Override
    protected void setup(Context context) {
        Configuration configuration =
                context.getConfiguration();
 
        // -1 means that no limit is applied.
        maxUsersPerItem = configuration.getInt(
                "retailsimilarity.max.users.per.item",
                -1
        );
    }
 
    /**
     * Deduplicates the users related to the same item and behavior.
     */
    @Override
    protected void reduce(
            ItemBehaviorWritable key,
            Iterable<LongWritable> values,
            Context context
    ) throws IOException, InterruptedException {
 
        // HashSet automatically removes duplicate user IDs.
        Set<Long> distinctUsers = new HashSet<>();
 
        boolean aboveMaximum = false;
 
        for (LongWritable value : values) {
            if (!aboveMaximum) {
                distinctUsers.add(value.get());
 
                // Very popular items may generate too many pairs in Job 2.
                if (maxUsersPerItem > 0
&& distinctUsers.size() > maxUsersPerItem) {
 
                    aboveMaximum = true;
                    distinctUsers.clear();
                }
            }
        }
 
        if (aboveMaximum) {
            context.getCounter(
                    ItemCounters.ITEMS_ABOVE_MAX_USERS
            ).increment(1);
 
            return;
        }
 
        // At least two users are needed to generate a pair.
        if (distinctUsers.size() < 2) {
            context.getCounter(
                    ItemCounters.ITEMS_WITH_LESS_THAN_TWO_USERS
            ).increment(1);
 
            return;
        }
 
        outputValue.setUsers(distinctUsers);
        context.write(key, outputValue);
 
        context.getCounter(ItemCounters.EMITTED_ITEMS)
                .increment(1);
    }
}