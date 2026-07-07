package retailsimilarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import retailsimilarity.job1.BehaviorInversionMapper;
import retailsimilarity.job1.DistinctUsersReducer;
import retailsimilarity.job2.PairGenerationMapper;
import retailsimilarity.job2.SimilarityCombiner;
import retailsimilarity.job2.SimilarityReducer;
import retailsimilarity.writable.ItemBehaviorWritable;
import retailsimilarity.writable.SimilarityWritable;
import retailsimilarity.writable.UserListWritable;
import retailsimilarity.writable.UserPairWritable;

/**
 * Driver class that configures and executes
 * the two MapReduce jobs.
 * Job 1 groups distinct users by item and behavior.
 * Job 2 generates user pairs and calculates their similarity.
 */
public class RetailSimilarityDriver
        extends Configured
        implements Tool {

    /**
     * Executes the two jobs in sequence.
     */
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: RetailSimilarityDriver "
                    + "<input> <intermediate-output> <final-output>"
            );

            return 2;
        }

        Configuration configuration = getConf();

        Path inputPath = new Path(args[0]);
        Path intermediatePath = new Path(args[1]);
        Path finalOutputPath = new Path(args[2]);

        boolean overwrite = configuration.getBoolean(
                "retailsimilarity.overwrite",
                false
        );

        // Deletes previous outputs when overwrite is enabled.
        if (overwrite) {
            deleteIfExists(intermediatePath, configuration);
            deleteIfExists(finalOutputPath, configuration);
        }

        /*
         * Local mode uses one reducer by default.
         *
         * YARN mode uses:
         * - 2 reducers for Job 1
         * - 4 reducers for Job 2
         *
         * Both values can be changed using -D.
         */
        String framework = configuration.get(
                "mapreduce.framework.name",
                "local"
        );

        boolean localMode =
                "local".equalsIgnoreCase(framework);

        int job1Reducers = configuration.getInt(
                "retailsimilarity.job1.reducers",
                localMode ? 1 : 2
        );

        int job2Reducers = configuration.getInt(
                "retailsimilarity.job2.reducers",
                localMode ? 1 : 4
        );

        System.out.println(
                "Execution framework: " + framework
        );

        System.out.println(
                "Job 1 reducers: " + job1Reducers
        );

        System.out.println(
                "Job 2 reducers: " + job2Reducers
        );

        // Creates and executes the first job.
        Job job1 = createFirstJob(
                configuration,
                inputPath,
                intermediatePath,
                job1Reducers
        );

        boolean firstJobSucceeded =
                job1.waitForCompletion(true);

        if (!firstJobSucceeded) {
            System.err.println("Job 1 failed.");
            return 1;
        }

        // Job 2 starts only if Job 1 completes successfully.
        Job job2 = createSecondJob(
                configuration,
                intermediatePath,
                finalOutputPath,
                job2Reducers
        );

        boolean secondJobSucceeded =
                job2.waitForCompletion(true);

        if (!secondJobSucceeded) {
            System.err.println("Job 2 failed.");
            return 1;
        }

        return 0;
    }

    /**
     * Configures the job that groups distinct users
     * by item and behavior.
     */
    private Job createFirstJob(
            Configuration configuration,
            Path inputPath,
            Path outputPath,
            int reducers
    ) throws Exception {

        Job job = Job.getInstance(
                configuration,
                "Retail similarity - group users by item"
        );

        job.setJarByClass(RetailSimilarityDriver.class);

        job.setMapperClass(
                BehaviorInversionMapper.class
        );

        job.setReducerClass(
                DistinctUsersReducer.class
        );

        job.setMapOutputKeyClass(
                ItemBehaviorWritable.class
        );

        job.setMapOutputValueClass(
                LongWritable.class
        );

        job.setOutputKeyClass(
                ItemBehaviorWritable.class
        );

        job.setOutputValueClass(
                UserListWritable.class
        );

        job.setInputFormatClass(
                TextInputFormat.class
        );

        /*
         * The intermediate output is stored as a SequenceFile
         * to preserve the custom Writable objects.
         */
        job.setOutputFormatClass(
                SequenceFileOutputFormat.class
        );

        job.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(
                job,
                inputPath
        );

        FileOutputFormat.setOutputPath(
                job,
                outputPath
        );

        return job;
    }

    /**
     * Configures the job that generates user pairs
     * and calculates their similarity.
     */
    private Job createSecondJob(
            Configuration configuration,
            Path inputPath,
            Path outputPath,
            int reducers
    ) throws Exception {

        Job job = Job.getInstance(
                configuration,
                "Retail similarity - generate user pairs"
        );

        job.setJarByClass(RetailSimilarityDriver.class);

        job.setMapperClass(
                PairGenerationMapper.class
        );

        /*
         * The combiner performs a local sum
         * before the Shuffle and Sort phase.
         */
        job.setCombinerClass(
                SimilarityCombiner.class
        );

        job.setReducerClass(
                SimilarityReducer.class
        );

        job.setMapOutputKeyClass(
                UserPairWritable.class
        );

        job.setMapOutputValueClass(
                SimilarityWritable.class
        );

        job.setOutputKeyClass(
                UserPairWritable.class
        );

        job.setOutputValueClass(
                SimilarityWritable.class
        );

        job.setInputFormatClass(
                SequenceFileInputFormat.class
        );

        job.setOutputFormatClass(
                TextOutputFormat.class
        );

        job.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(
                job,
                inputPath
        );

        FileOutputFormat.setOutputPath(
                job,
                outputPath
        );

        return job;
    }

    /**
     * Deletes an output path if it already exists.
     */
    private void deleteIfExists(
            Path path,
            Configuration configuration
    ) throws Exception {

        FileSystem fileSystem =
                path.getFileSystem(configuration);

        if (fileSystem.exists(path)) {
            boolean deleted =
                    fileSystem.delete(path, true);

            if (!deleted) {
                throw new IllegalStateException(
                        "Unable to delete path: " + path
                );
            }
        }
    }

    /**
     * Starts the application using ToolRunner.
     */
    public static void main(String[] args)
            throws Exception {

        int exitCode = ToolRunner.run(
                new Configuration(),
                new RetailSimilarityDriver(),
                args
        );

        System.exit(exitCode);
    }
}