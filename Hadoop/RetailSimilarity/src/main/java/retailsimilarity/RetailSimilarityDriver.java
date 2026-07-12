package retailsimilarity;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import retailsimilarity.job1.BehaviorInversionMapper;
import retailsimilarity.job1.DistinctUserCombiner;
import retailsimilarity.job1.DistinctUsersReducer;
import retailsimilarity.job2.PairGenerationMapper;
import retailsimilarity.job2.SimilarityCombiner;
import retailsimilarity.job2.SimilarityReducer;
import retailsimilarity.writable.ItemBehaviorWritable;
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserListWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Runs the two-job retail user-similarity pipeline.
 *
 * Generic Hadoop -D options are used for all thresholds and weights.
 */
public class RetailSimilarityDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            printUsage();
            return 2;
        }

        validateConfiguration();

        Path inputPath = new Path(args[0]);
        Path job1OutputPath = new Path(args[1]);
        Path job2OutputPath = new Path(args[2]);

        boolean overwrite = getConf().getBoolean(
                RetailSimilarityConfig.OVERWRITE_OUTPUTS,
                false
        );

        prepareOutputPath(job1OutputPath, overwrite);
        prepareOutputPath(job2OutputPath, overwrite);

        Job job1 = buildJob1(inputPath, job1OutputPath);
        if (!job1.waitForCompletion(true)) {
            return 1;
        }
        printJob1Summary(job1);

        Job job2 = buildJob2(job1OutputPath, job2OutputPath);
        if (!job2.waitForCompletion(true)) {
            return 1;
        }
        printJob2Summary(job2);

        return 0;
    }

    private Job buildJob1(Path input, Path output) throws IOException {
        Job job = Job.getInstance(
                getConf(),
                "retail-similarity-job1-posting-lists"
        );
        job.setJarByClass(RetailSimilarityDriver.class);

        job.setMapperClass(BehaviorInversionMapper.class);
        if (getConf().getBoolean(
                RetailSimilarityConfig.JOB1_COMBINER_ENABLED,
                true
        )) {
            job.setCombinerClass(DistinctUserCombiner.class);
        }
        job.setReducerClass(DistinctUsersReducer.class);

        job.setMapOutputKeyClass(ItemBehaviorWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(ItemBehaviorWritable.class);
        job.setOutputValueClass(UserListWritable.class);

        job.setNumReduceTasks(getConf().getInt(
                RetailSimilarityConfig.JOB1_REDUCERS,
                RetailSimilarityConfig.DEFAULT_JOB1_REDUCERS
        ));

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(
                job,
                SequenceFile.CompressionType.BLOCK
        );

        enableMapOutputCompression(job);
        return job;
    }

    private Job buildJob2(Path input, Path output) throws IOException {
        Job job = Job.getInstance(
                getConf(),
                "retail-similarity-job2-bounded-pairs"
        );
        job.setJarByClass(RetailSimilarityDriver.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(PairGenerationMapper.class);
        job.setCombinerClass(SimilarityCombiner.class);
        job.setReducerClass(SimilarityReducer.class);

        job.setMapOutputKeyClass(UserPairWritable.class);
        job.setMapOutputValueClass(SimilarityWritable.class);
        job.setOutputKeyClass(UserPairWritable.class);
        job.setOutputValueClass(SimilarityWritable.class);

        job.setNumReduceTasks(getConf().getInt(
                RetailSimilarityConfig.JOB2_REDUCERS,
                RetailSimilarityConfig.DEFAULT_JOB2_REDUCERS
        ));

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        enableMapOutputCompression(job);
        return job;
    }

    private void enableMapOutputCompression(Job job) {
        job.getConfiguration().setBoolean(
                "mapreduce.map.output.compress",
                true
        );
        if (job.getConfiguration().get(
                "mapreduce.map.output.compress.codec"
        ) == null) {
            job.getConfiguration().set(
                    "mapreduce.map.output.compress.codec",
                    DefaultCodec.class.getName()
            );
        }
    }

    private void prepareOutputPath(Path path, boolean overwrite)
            throws IOException {
        FileSystem fileSystem = path.getFileSystem(getConf());
        if (!fileSystem.exists(path)) {
            return;
        }
        if (!overwrite) {
            throw new IOException(
                    "Output path already exists: " + path
                            + ". Set -D"
                            + RetailSimilarityConfig.OVERWRITE_OUTPUTS
                            + "=true to delete it."
            );
        }
        if (!fileSystem.delete(path, true)) {
            throw new IOException("Cannot delete output path: " + path);
        }
    }

    private void validateConfiguration() {
        validateAtLeast(
                RetailSimilarityConfig.EXACT_MAX_USERS_BUY,
                getConf().getInt(
                        RetailSimilarityConfig.EXACT_MAX_USERS_BUY,
                        RetailSimilarityConfig.DEFAULT_EXACT_MAX_USERS_BUY
                ),
                -1
        );
        validateAtLeast(
                RetailSimilarityConfig.EXACT_MAX_USERS_PV,
                getConf().getInt(
                        RetailSimilarityConfig.EXACT_MAX_USERS_PV,
                        RetailSimilarityConfig.DEFAULT_EXACT_MAX_USERS_PV
                ),
                -1
        );
        validateAtLeast(
                RetailSimilarityConfig.MAX_NEIGHBOURS_BUY,
                getConf().getInt(
                        RetailSimilarityConfig.MAX_NEIGHBOURS_BUY,
                        RetailSimilarityConfig.DEFAULT_MAX_NEIGHBOURS_BUY
                ),
                0
        );
        validateAtLeast(
                RetailSimilarityConfig.MAX_NEIGHBOURS_PV,
                getConf().getInt(
                        RetailSimilarityConfig.MAX_NEIGHBOURS_PV,
                        RetailSimilarityConfig.DEFAULT_MAX_NEIGHBOURS_PV
                ),
                0
        );

        boolean iufEnabled = getConf().getBoolean(
                RetailSimilarityConfig.IUF_ENABLED,
                true
        );
        if (iufEnabled && getConf().getLong(
                RetailSimilarityConfig.TOTAL_USERS,
                -1L
        ) <= 0L) {
            throw new IllegalArgumentException(
                    "When IUF is enabled, set -D"
                            + RetailSimilarityConfig.TOTAL_USERS
                            + "=<distinct-user-count>"
            );
        }
    }

    private static void validateAtLeast(
            String key,
            int value,
            int minimum
    ) {
        if (value < minimum) {
            throw new IllegalArgumentException(
                    key + " must be >= " + minimum + ", found " + value
            );
        }
    }

    private static void printJob1Summary(Job job) throws IOException {
        System.out.println("\n=== Job 1 threshold forecast ===");
        printCounter(
                job,
                DistinctUsersReducer.ItemCounters.DISTINCT_BUY_EDGES
        );
        printCounter(
                job,
                DistinctUsersReducer.ItemCounters.DISTINCT_PV_EDGES
        );
        printCounter(
                job,
                DistinctUsersReducer.ItemCounters.POTENTIAL_BUY_ALL_PAIRS
        );
        printCounter(
                job,
                DistinctUsersReducer.ItemCounters.POTENTIAL_PV_ALL_PAIRS
        );
        printCounter(
                job,
                DistinctUsersReducer.ItemCounters.PLANNED_BUY_PAIR_CONTRIBUTIONS
        );
        printCounter(
                job,
                DistinctUsersReducer.ItemCounters.PLANNED_PV_PAIR_CONTRIBUTIONS
        );
    }

    private static void printJob2Summary(Job job) throws IOException {
        System.out.println("\n=== Job 2 generated output ===");
        printCounter(job, PairGenerationMapper.PairCounters.GENERATED_BUY_PAIRS);
        printCounter(job, PairGenerationMapper.PairCounters.GENERATED_PV_PAIRS);
        printCounter(job, PairGenerationMapper.PairCounters.EXACT_BUY_POSTING_LISTS);
        printCounter(job, PairGenerationMapper.PairCounters.EXACT_PV_POSTING_LISTS);
        printCounter(job, PairGenerationMapper.PairCounters.SAMPLED_BUY_POSTING_LISTS);
        printCounter(job, PairGenerationMapper.PairCounters.SAMPLED_PV_POSTING_LISTS);
        printCounter(job, SimilarityReducer.SimilarityCounters.EMITTED_PAIRS);
        printCounter(job, SimilarityReducer.SimilarityCounters.FILTERED_BY_SUPPORT);
        printCounter(job, SimilarityReducer.SimilarityCounters.FILTERED_BY_SCORE);
    }

    private static void printCounter(Job job, Enum<?> counterName)
            throws IOException {
        Counter counter = job.getCounters().findCounter(counterName);
        System.out.println(counterName.name() + "=" + counter.getValue());
    }

    private static void printUsage() {
        System.err.println(
                "Usage: hadoop jar <jar> "
                        + RetailSimilarityDriver.class.getName()
                        + " [generic -D options] <input> <job1-output> <job2-output>"
        );
        System.err.println("Required when IUF is enabled:");
        System.err.println(
                "  -D" + RetailSimilarityConfig.TOTAL_USERS
                        + "=<number of distinct users>"
        );
        System.err.println("Main threshold options:");
        System.err.println(
                "  -D" + RetailSimilarityConfig.EXACT_MAX_USERS_BUY
                        + "=<n>"
        );
        System.err.println(
                "  -D" + RetailSimilarityConfig.EXACT_MAX_USERS_PV
                        + "=<n>"
        );
        System.err.println(
                "  -D" + RetailSimilarityConfig.MAX_NEIGHBOURS_BUY
                        + "=<k>"
        );
        System.err.println(
                "  -D" + RetailSimilarityConfig.MAX_NEIGHBOURS_PV
                        + "=<k>"
        );
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(
                new RetailSimilarityDriver(),
                args
        );
        System.exit(exitCode);
    }
}