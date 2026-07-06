package retailsimilarity.job2;
 
import java.io.IOException;
 
import org.apache.hadoop.mapreduce.Reducer;
 
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserPairWritable;
 
/**
 * Combiner of job 2
 * A combiner works on a single output of a certain mapper: it sums up all the values of (key,value) pairs with same key
 *
 * The result is just one (key,value) pair for each different key
 *
 * Example:
 *
 * (U1,U2) -> (1,0), (1,0), (0,1)
 *
 * becomes (locally):
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