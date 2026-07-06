package retailsimilarity.job1;
 
import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
import retailsimilarity.writable.ItemBehaviorWritable;
 
/**
 * Reads user interactions and keeps only buy and fav events.
 *
 * Input:  userId,itemId,behavior,timestamp
 * Output: (itemId, behavior) -> userId
 */
public class BehaviorInversionMapper extends Mapper<
        LongWritable,
        Text,
        ItemBehaviorWritable,
        LongWritable> {
 
    // Counters used to check the input data.
    public enum InputCounters {
        VALID_BUY_RECORDS,
        VALID_FAV_RECORDS,
        FILTERED_BEHAVIORS,
        INVALID_RECORDS,
        HEADER_RECORDS
    }
 
    private int userIdIndex;
    private int itemIdIndex;
    private int behaviorIndex;
 
    private boolean inputHasHeader;
    private String delimiterRegex;
 
    // Reuse the same objects for every mapper output.
    private final ItemBehaviorWritable outputKey =
            new ItemBehaviorWritable();
 
    private final LongWritable outputValue =
            new LongWritable();
 
    /**
     * Reads the mapper configuration before processing the input.
     */
    @Override
    protected void setup(Context context) {
        Configuration configuration = context.getConfiguration();
 
        String delimiter = configuration.get(
                "retailsimilarity.input.delimiter",
                ","
        );
 
        delimiterRegex = Pattern.quote(delimiter);
 
        userIdIndex = configuration.getInt(
                "retailsimilarity.input.user.index",
                0
        );
 
        itemIdIndex = configuration.getInt(
                "retailsimilarity.input.item.index",
                1
        );
 
        // Category ID was removed, so behavior is column 2.
        behaviorIndex = configuration.getInt(
                "retailsimilarity.input.behavior.index",
                2
        );
 
        inputHasHeader = configuration.getBoolean(
                "retailsimilarity.input.has.header",
                false
        );
    }
 
    /**
     * Processes one CSV row.
     */
    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {
 
        String line = value.toString().trim();
 
        // Ignore empty rows.
        if (line.isEmpty()) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1);
            return;
        }
 
        String[] fields = line.split(delimiterRegex, -1);
 
        int requiredColumns = Math.max(
                userIdIndex,
                Math.max(itemIdIndex, behaviorIndex)
        ) + 1;
 
        // Ignore rows with missing fields.
        if (fields.length < requiredColumns) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1);
            return;
        }
 
        // Skip the header when it is present.
        if (inputHasHeader
                && isHeader(
                        fields[userIdIndex],
                        fields[itemIdIndex]
                )) {
 
            context.getCounter(InputCounters.HEADER_RECORDS)
                    .increment(1);
            return;
        }
 
        try {
            long userId = Long.parseLong(
                    fields[userIdIndex].trim()
            );
 
            long itemId = Long.parseLong(
                    fields[itemIdIndex].trim()
            );
 
            String behavior = fields[behaviorIndex]
                    .trim()
                    .toLowerCase(Locale.ROOT);
 
            byte behaviorCode;
 
            // Only buy and fav are useful for the similarity.
            if ("buy".equals(behavior)) {
                behaviorCode = ItemBehaviorWritable.BUY;
 
                context.getCounter(
                        InputCounters.VALID_BUY_RECORDS
                ).increment(1);
 
            } else if ("fav".equals(behavior)) {
                behaviorCode = ItemBehaviorWritable.FAV;
 
                context.getCounter(
                        InputCounters.VALID_FAV_RECORDS
                ).increment(1);
 
            } else {
                context.getCounter(
                        InputCounters.FILTERED_BEHAVIORS
                ).increment(1);
                return;
            }
 
            // Group users by item and behavior.
            outputKey.set(itemId, behaviorCode);
            outputValue.set(userId);
 
            context.write(outputKey, outputValue);
 
        } catch (NumberFormatException exception) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1);
        }
    }
 
    /**
     * Checks if the current row is the CSV header.
     */
    private boolean isHeader(
            String userField,
            String itemField
    ) {
        String normalizedUser =
                userField.trim().toLowerCase(Locale.ROOT);
 
        String normalizedItem =
                itemField.trim().toLowerCase(Locale.ROOT);
 
        return normalizedUser.contains("user")
                || normalizedItem.contains("item");
    }
}