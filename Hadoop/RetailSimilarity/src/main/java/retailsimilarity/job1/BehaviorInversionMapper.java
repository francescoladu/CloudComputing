package retailsimilarity.job1;

import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import retailsimilarity.RetailSimilarityConfig;
import retailsimilarity.writable.ItemBehaviorWritable;

/**
 * Input:  userId,itemId,behavior,timestamp
 * Output: (itemId, behavior) -> userId
 *
 * Only buy and pv records are retained.
 */
public class BehaviorInversionMapper extends Mapper<
        LongWritable,
        Text,
        ItemBehaviorWritable,
        LongWritable> {

    public enum InputCounters {
        VALID_BUY_RECORDS,
        VALID_PV_RECORDS,
        FILTERED_BEHAVIORS,
        INVALID_RECORDS,
        HEADER_RECORDS
    }

    private int userIdIndex;
    private int itemIdIndex;
    private int behaviorIndex;
    private boolean inputHasHeader;
    private String delimiterRegex;

    private final ItemBehaviorWritable outputKey =
            new ItemBehaviorWritable();
    private final LongWritable outputValue =
            new LongWritable();

    @Override
    protected void setup(Context context) {
        Configuration configuration = context.getConfiguration();

        String delimiter = configuration.get(
                RetailSimilarityConfig.INPUT_DELIMITER,
                ","
        );
        delimiterRegex = Pattern.quote(delimiter);

        userIdIndex = configuration.getInt(
                RetailSimilarityConfig.INPUT_USER_INDEX,
                0
        );
        itemIdIndex = configuration.getInt(
                RetailSimilarityConfig.INPUT_ITEM_INDEX,
                1
        );
        behaviorIndex = configuration.getInt(
                RetailSimilarityConfig.INPUT_BEHAVIOR_INDEX,
                2
        );
        inputHasHeader = configuration.getBoolean(
                RetailSimilarityConfig.INPUT_HAS_HEADER,
                false
        );
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1L);
            return;
        }

        String[] fields = line.split(delimiterRegex, -1);
        int requiredColumns = Math.max(
                userIdIndex,
                Math.max(itemIdIndex, behaviorIndex)
        ) + 1;

        if (fields.length < requiredColumns) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1L);
            return;
        }

        if (inputHasHeader
                && isHeader(fields[userIdIndex], fields[itemIdIndex])) {
            context.getCounter(InputCounters.HEADER_RECORDS)
                    .increment(1L);
            return;
        }

        try {
            long userId = Long.parseLong(fields[userIdIndex].trim());
            long itemId = Long.parseLong(fields[itemIdIndex].trim());
            String behavior = fields[behaviorIndex]
                    .trim()
                    .toLowerCase(Locale.ROOT);

            byte behaviorCode;
            if ("buy".equals(behavior)) {
                behaviorCode = ItemBehaviorWritable.BUY;
                context.getCounter(InputCounters.VALID_BUY_RECORDS)
                        .increment(1L);
            } else if ("pv".equals(behavior)) {
                behaviorCode = ItemBehaviorWritable.PV;
                context.getCounter(InputCounters.VALID_PV_RECORDS)
                        .increment(1L);
            } else {
                context.getCounter(InputCounters.FILTERED_BEHAVIORS)
                        .increment(1L);
                return;
            }

            outputKey.set(itemId, behaviorCode);
            outputValue.set(userId);
            context.write(outputKey, outputValue);

        } catch (NumberFormatException exception) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1L);
        }
    }

    private boolean isHeader(String userField, String itemField) {
        String normalizedUser =
                userField.trim().toLowerCase(Locale.ROOT);
        String normalizedItem =
                itemField.trim().toLowerCase(Locale.ROOT);

        return normalizedUser.contains("user")
                || normalizedItem.contains("item");
    }
}
