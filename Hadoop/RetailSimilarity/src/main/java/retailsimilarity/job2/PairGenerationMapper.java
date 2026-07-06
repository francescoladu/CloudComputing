package retailsimilarity.job2;
 
import java.io.IOException;
 
import org.apache.hadoop.mapreduce.Mapper;
 
import retailsimilarity.writable.ItemBehaviorWritable;
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserListWritable;
import retailsimilarity.writable.UserPairWritable;
 
/**
 * Mapper of second job
 *
 * Input (from reducer of the first job): (itemId, behavior) -> [distinct users]
 *
 * Output: (user1, user2) -> (1, 0) per buy
 *         (user1, user2) -> (0, 1) per fav
 *
 *         (user1, user3) -> (0, 1) per buy
 *         (user1, user3) -> (0, 1) per fav
 *
 *         ...
 *         ...
 */
public class PairGenerationMapper extends Mapper<
        ItemBehaviorWritable,
        UserListWritable,
        UserPairWritable,
        SimilarityWritable> {
 
    public enum PairCounters {
        GENERATED_BUY_PAIRS,
        GENERATED_FAV_PAIRS
    }
 
    private final UserPairWritable outputKey =
            new UserPairWritable();
 
    private final SimilarityWritable buyContribution =
            new SimilarityWritable(1, 0);
 
    private final SimilarityWritable favContribution =
            new SimilarityWritable(0, 1);
 
    @Override
    protected void map(
            ItemBehaviorWritable key,
            UserListWritable value,
            Context context
    ) throws IOException, InterruptedException {
 
        long[] users = value.getUsers();
 
        /*
         * There will be generated all the possible combinations of pair users figuring inside the list
         *
         * Thanks to job1, the users are already sorted inside the list: in this way we are sure to generate all and only significant pairs
         */
        for (int firstIndex = 0;
             firstIndex < users.length - 1;
             firstIndex++) {
 
            for (int secondIndex = firstIndex + 1;
                 secondIndex < users.length;
                 secondIndex++) {
 
                // output key is the pair fo users
 
                outputKey.set(
                        users[firstIndex],
                        users[secondIndex]
                );
 
                // output value is either the SimilarityWritable buyContribution object or the SimilarityWritable favContribution object
 
                if (key.getBehavior()
                        == ItemBehaviorWritable.BUY) {
 
                    context.write(
                            outputKey,
                            buyContribution
                    );
 
                    context.getCounter(
                            PairCounters.GENERATED_BUY_PAIRS
                    ).increment(1);
 
                } else if (key.getBehavior()
                        == ItemBehaviorWritable.FAV) {
 
                    context.write(
                            outputKey,
                            favContribution
                    );
 
                    context.getCounter(
                            PairCounters.GENERATED_FAV_PAIRS
                    ).increment(1);
                }
            }
        }
    }
}