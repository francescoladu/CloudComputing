package retailsimilarity.job2;
 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
 
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserPairWritable;
 
/**
 * Reducer of second job
 *
 * After the Shuffle and Sort phase, the reducer inherits a subset of of the total user pairs with the respective list of values
 * The reducer sums up all the values for each user pair
 *
 * Input:
 *
 * (user1, user2) -> [(buy_i, pv_i), (buy_j, pv_j), ...]
 *
 * Output:
 *
 * (user1, user2) -> (commonBuy, commonPv)
 */
public class SimilarityReducer extends Reducer<
        UserPairWritable,
        SimilarityWritable,
        UserPairWritable,
        SimilarityWritable> {
 
    private long minimumBuySimilarity;
    private long minimumPvSimilarity;
 
    private final SimilarityWritable outputValue =
            new SimilarityWritable();
 
    /**
     * setup() function to configure min_sup tresholds to filter (key,value) pairs with small values
     *
     * If both tresholds == 0, then all pairs are accepted
     */
 
    @Override
    protected void setup(Context context) {
        Configuration configuration =
                context.getConfiguration();
 
        minimumBuySimilarity = configuration.getLong(
                "retailsimilarity.min.buy",
                0L
        );
 
        minimumPvSimilarity = configuration.getLong(
                "retailsimilarity.min.pv",
                0L
        );
    }
 
    @Override
    protected void reduce(
            UserPairWritable key,
            Iterable<SimilarityWritable> values,
            Context context
    ) throws IOException, InterruptedException {
 
        long buySum = 0;
        long pvSum = 0;
 
        //cumulative sum
        for (SimilarityWritable value : values) {
            buySum += value.getBuyCount();
            pvSum += value.getPvCount();
        }
 
        //small values filtering
        if (buySum < minimumBuySimilarity
                && pvSum < minimumPvSimilarity) {
            return;
        }
 
        outputValue.set(buySum, pvSum);
 
        context.write(key, outputValue);
    }
}