package retailsimilarity.job2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Input:
 *
 * (user1, user2) -> [(buy, fav), ...]
 *
 * Output:
 *
 * (user1, user2) -> (commonBuy, commonFav)
 */
public class SimilarityReducer extends Reducer<
        UserPairWritable,
        SimilarityWritable,
        UserPairWritable,
        SimilarityWritable> {

    private long minimumBuySimilarity;
    private long minimumFavSimilarity;

    private final SimilarityWritable outputValue =
            new SimilarityWritable();

    /**
     * Legge eventuali soglie configurabili.
     *
     * Con entrambe a zero vengono emesse tutte le coppie.
     */
    @Override
    protected void setup(Context context) {
        Configuration configuration =
                context.getConfiguration();

        minimumBuySimilarity = configuration.getLong(
                "retailsimilarity.min.buy",
                0L
        );

        minimumFavSimilarity = configuration.getLong(
                "retailsimilarity.min.fav",
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
        long favSum = 0;

        for (SimilarityWritable value : values) {
            buySum += value.getBuyCount();
            favSum += value.getFavCount();
        }

        if (buySum < minimumBuySimilarity
                || favSum < minimumFavSimilarity) {
            return;
        }

        outputValue.set(buySum, favSum);

        context.write(key, outputValue);
    }
}