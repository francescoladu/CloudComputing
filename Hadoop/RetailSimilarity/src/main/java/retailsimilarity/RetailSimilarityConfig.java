package retailsimilarity;

/**
 * Centralized configuration keys used by the MapReduce pipeline.
 */
public final class RetailSimilarityConfig {

    private RetailSimilarityConfig() {
    }

    public static final String INPUT_DELIMITER =
            "retailsimilarity.input.delimiter";
    public static final String INPUT_USER_INDEX =
            "retailsimilarity.input.user.index";
    public static final String INPUT_ITEM_INDEX =
            "retailsimilarity.input.item.index";
    public static final String INPUT_BEHAVIOR_INDEX =
            "retailsimilarity.input.behavior.index";
    public static final String INPUT_HAS_HEADER =
            "retailsimilarity.input.has.header";

    public static final String JOB1_REDUCERS =
            "retailsimilarity.job1.reducers";
    public static final String JOB2_REDUCERS =
            "retailsimilarity.job2.reducers";
    public static final String JOB3_REDUCERS =
            "retailsimilarity.job3.reducers";
    public static final String JOB1_COMBINER_ENABLED =
            "retailsimilarity.job1.combiner.enabled";
    public static final String OVERWRITE_OUTPUTS =
            "retailsimilarity.overwrite.outputs";

    /**
     * Optional emergency guard in Job 1. A value of -1 disables it.
     * If enabled, posting lists larger than this value are discarded.
     */
    public static final String MAX_USERS_PER_ITEM =
            "retailsimilarity.max.users.per.item";

    public static final String EXACT_MAX_USERS_BUY =
            "retailsimilarity.pairs.exact.max.users.buy";
    public static final String EXACT_MAX_USERS_PV =
            "retailsimilarity.pairs.exact.max.users.pv";
    public static final String MAX_NEIGHBOURS_BUY =
            "retailsimilarity.pairs.max.neighbours.buy";
    public static final String MAX_NEIGHBOURS_PV =
            "retailsimilarity.pairs.max.neighbours.pv";
    public static final String SAMPLING_SEED =
            "retailsimilarity.pairs.sample.seed";

    public static final String IUF_ENABLED =
            "retailsimilarity.iuf.enabled";
    public static final String TOTAL_USERS =
            "retailsimilarity.total.users";
    public static final String IUF_SMOOTHING =
            "retailsimilarity.iuf.smoothing";
    public static final String IUF_MAX_WEIGHT =
            "retailsimilarity.iuf.max.weight";

    public static final String BUY_BEHAVIOR_WEIGHT =
            "retailsimilarity.behavior.weight.buy";
    public static final String PV_BEHAVIOR_WEIGHT =
            "retailsimilarity.behavior.weight.pv";

    public static final String MIN_BUY_COUNT =
            "retailsimilarity.min.buy.count";
    public static final String MIN_PV_COUNT =
            "retailsimilarity.min.pv.count";
    public static final String MIN_SCORE =
            "retailsimilarity.min.score";

    public static final String OUTPUT_TOP_K =
            "retailsimilarity.output.top.k";

    public static final int DEFAULT_JOB1_REDUCERS = 16;
    public static final int DEFAULT_JOB2_REDUCERS = 32;
    public static final int DEFAULT_JOB3_REDUCERS = 32;
    public static final int DEFAULT_OUTPUT_TOP_K = 50;

    public static final int DEFAULT_EXACT_MAX_USERS_BUY = 21;
    public static final int DEFAULT_EXACT_MAX_USERS_PV = 5;
    public static final int DEFAULT_MAX_NEIGHBOURS_BUY = 10;
    public static final int DEFAULT_MAX_NEIGHBOURS_PV = 2;

    public static final long DEFAULT_SAMPLING_SEED =
            0x6A09E667F3BCC909L;

    public static final double DEFAULT_IUF_SMOOTHING = 1.0;
    public static final double DEFAULT_IUF_MAX_WEIGHT = 8.0;
    public static final double DEFAULT_BUY_BEHAVIOR_WEIGHT = 1.0;
    public static final double DEFAULT_PV_BEHAVIOR_WEIGHT = 0.2;
}
