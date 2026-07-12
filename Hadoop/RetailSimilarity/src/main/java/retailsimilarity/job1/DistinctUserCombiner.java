package retailsimilarity.job1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import retailsimilarity.writable.ItemBehaviorWritable;

/**
 * Optional mapper-local deduplication for Job 1.
 */
public class DistinctUserCombiner extends Reducer<
        ItemBehaviorWritable,
        LongWritable,
        ItemBehaviorWritable,
        LongWritable> {

    private final LongWritable outputValue = new LongWritable();

    @Override
    protected void reduce(
            ItemBehaviorWritable key,
            Iterable<LongWritable> values,
            Context context
    ) throws IOException, InterruptedException {

        Set<Long> distinctUsers = new HashSet<>();
        for (LongWritable value : values) {
            distinctUsers.add(value.get());
        }

        for (Long userId : distinctUsers) {
            outputValue.set(userId);
            context.write(key, outputValue);
        }
    }
}
