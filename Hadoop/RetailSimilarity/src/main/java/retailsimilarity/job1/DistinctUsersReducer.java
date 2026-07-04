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
 * Input:
 *
 * (itemId, behavior) -> [userId, userId, ...]
 *
 * Output:
 *
 * (itemId, behavior) -> [utenti distinti e ordinati]
 */
public class DistinctUsersReducer extends Reducer<
        ItemBehaviorWritable,
        LongWritable,
        ItemBehaviorWritable,
        UserListWritable> {

    public enum ItemCounters {
        EMITTED_ITEMS,
        ITEMS_WITH_LESS_THAN_TWO_USERS,
        ITEMS_ABOVE_MAX_USERS
    }

    private int maxUsersPerItem;

    private final UserListWritable outputValue =
            new UserListWritable();

    @Override
    protected void setup(Context context) {
        Configuration configuration =
                context.getConfiguration();

        /*
         * -1 significa nessun limite.
         *
         * Impostare un limite produce un risultato approssimato,
         * perché gli item troppo popolari vengono esclusi.
         */
        maxUsersPerItem = configuration.getInt(
                "retailsimilarity.max.users.per.item",
                -1
        );
    }

    @Override
    protected void reduce(
            ItemBehaviorWritable key,
            Iterable<LongWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        Set<Long> distinctUsers = new HashSet<>();

        boolean aboveMaximum = false;

        for (LongWritable value : values) {
            if (!aboveMaximum) {
                distinctUsers.add(value.get());

                if (maxUsersPerItem > 0
                        && distinctUsers.size()
                        > maxUsersPerItem) {

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

        /*
         * Con un solo utente non si può generare
         * nessuna coppia.
         */
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