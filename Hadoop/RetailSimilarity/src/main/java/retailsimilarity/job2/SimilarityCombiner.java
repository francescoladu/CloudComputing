package retailsimilarity.job2;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Somma localmente i contributi prodotti dal mapper.
 *
 * Esempio:
 *
 * (U1,U2) -> (1,0), (1,0), (0,1)
 *
 * diventa localmente:
 *
 * (U1,U2) -> (2,1)
 */
public class SimilarityCombiner extends Reducer<
        UserPairWritable,
        SimilarityWritable,
        UserPairWritable,
        SimilarityWritable> {

    private final SimilarityWritable outputValue =
            new SimilarityWritable();

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

        outputValue.set(buySum, favSum);

        context.write(key, outputValue);
    }
}