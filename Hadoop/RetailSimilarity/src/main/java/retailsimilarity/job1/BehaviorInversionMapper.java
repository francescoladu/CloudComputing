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
 * Input:
 *
 * userId,itemId,categoryId,behavior,timestamp
 *
 * Output:
 *
 * (itemId, behavior) -> userId
 */
public class BehaviorInversionMapper extends Mapper<
        LongWritable,
        Text,
        ItemBehaviorWritable,
        LongWritable> {

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

    private final ItemBehaviorWritable outputKey =
            new ItemBehaviorWritable();

    private final LongWritable outputValue =
            new LongWritable();

    /**
     * Feature setup():
     *
     * legge configurazione e parametri una sola volta,
     * prima che map() elabori i record.
     */
    @Override
    protected void setup(Context context) {
        Configuration configuration =
                context.getConfiguration();

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

        behaviorIndex = configuration.getInt(
                "retailsimilarity.input.behavior.index",
                3
        );

        inputHasHeader = configuration.getBoolean(
                "retailsimilarity.input.has.header",
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
                    .increment(1);

            return;
        }

        String[] fields = line.split(delimiterRegex, -1);

        int requiredColumns = Math.max(
                userIdIndex,
                Math.max(itemIdIndex, behaviorIndex)
        ) + 1;

        if (fields.length < requiredColumns) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1);

            return;
        }

        if (inputHasHeader
                && isHeader(fields[userIdIndex],
                            fields[itemIdIndex])) {

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

            outputKey.set(itemId, behaviorCode);
            outputValue.set(userId);

            context.write(outputKey, outputValue);

        } catch (NumberFormatException exception) {
            context.getCounter(InputCounters.INVALID_RECORDS)
                    .increment(1);
        }
    }

    private boolean isHeader(
            String userField,
            String itemField
    ) {
        String normalizedUser =
                userField.toLowerCase(Locale.ROOT);

        String normalizedItem =
                itemField.toLowerCase(Locale.ROOT);

        return normalizedUser.contains("user")
                || normalizedItem.contains("item");
    }
}