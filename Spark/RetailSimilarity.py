#!/usr/bin/env python3
"""
Spark implementation of the RetailSimilarity workflow.

Input format after removing Category ID:
    user_id,item_id,behavior,timestamp

Supported behaviors:
    buy
    pv

Final output:
    user1,user2<TAB>common_buy_items,common_viewed_items
"""

import argparse
import time
from itertools import combinations
from typing import Iterable, Optional, Set, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel


# Type aliases used to make the RDD transformations clearer.
ItemBehavior = Tuple[int, str]       # (item_id, behavior)
UserPair = Tuple[int, int]           # (user1, user2)
Similarity = Tuple[int, int]         # (common_buy_count, common_pv_count)


def parse_record(
    line: str,
    delimiter: str,
    user_index: int,
    item_index: int,
    behavior_index: int,
) -> Optional[Tuple[ItemBehavior, int]]:
    """
    Parses one input row.

    Input row: user_id,item_id,behavior,timestamp

    Output: ((item_id, behavior), user_id)

    Invalid rows, headers, and behaviors different from buy/pv are discarded.
    """
    line = line.strip()

    if not line:
        return None

    fields = line.split(delimiter)
    required_size = max(user_index, item_index, behavior_index) + 1

    if len(fields) < required_size:
        return None

    try:
        user_id = int(fields[user_index].strip())
        item_id = int(fields[item_index].strip())
    except ValueError:
        # This also removes a possible header row.
        return None

    behavior = fields[behavior_index].strip().lower()

    if behavior not in {"buy", "pv"}:
        return None

    return (item_id, behavior), user_id


def create_user_set(user_id: int) -> Set[int]:
    """
    Creates the first set for a new item/behavior key.
    """
    return {user_id}


def add_user(user_set: Set[int], user_id: int) -> Set[int]:
    """
    Adds a user to the existing set inside one partition.
    This removes duplicates automatically.
    """
    user_set.add(user_id)
    return user_set


def merge_user_sets(left: Set[int], right: Set[int]) -> Set[int]:

    """
    Merges two partial user sets coming from different partitions.
    The smaller set is merged into the larger one to reduce operations.
    """

    if len(left) < len(right):
        left, right = right, left
    left.update(right)
    return left


def item_is_usable(
    record: Tuple[ItemBehavior, Set[int]],
    max_users_per_item: int,
) -> bool:
    """
    Checks if an item/behavior group can generate user pairs.

    A group with less than two users cannot generate pairs.
    A very popular item can be discarded to avoid generating too many pairs.
    """
    users = record[1]

    if len(users) < 2:
        return False

    if max_users_per_item > 0 and len(users) > max_users_per_item:
        return False

    return True


def generate_pair_contributions(
    record: Tuple[ItemBehavior, Set[int]]
) -> Iterable[Tuple[UserPair, Similarity]]:
    """
    Generates all unordered user pairs for one item/behavior group.

    """
    (_, behavior), users = record

    ordered_users = sorted(users)

    if behavior == "buy":
        contribution = (1, 0)
    else:
        contribution = (0, 1)

    for first_user, second_user in combinations(ordered_users, 2):
        yield (first_user, second_user), contribution


def add_similarities(left: Similarity, right: Similarity) -> Similarity:
    """
    Sums two similarity values.

    """
    return left[0] + right[0], left[1] + right[1]


def passes_thresholds(
    record: Tuple[UserPair, Similarity],
    min_buy: int,
    min_pv: int,
) -> bool:
    """
    Keeps only pairs that pass the minimum thresholds.
    the pair is discarded only if both values are below threshold.

    """
    buy_count, pv_count = record[1]

    return buy_count >= min_buy or pv_count >= min_pv


def format_output(record: Tuple[UserPair, Similarity]) -> str:
    """
    Converts the final result into text format.

    """
    (first_user, second_user), (buy_count, pv_count) = record

    return f"{first_user},{second_user}\t{buy_count},{pv_count}"


def delete_output_if_needed(
    spark_context: SparkContext,
    output_path: str,
    overwrite: bool,
) -> None:
    """
    Deletes the output directory if it already exists and overwrite is enabled.

    Works with local filesystem and HDFS paths.
    """
    java_path = spark_context._jvm.org.apache.hadoop.fs.Path(output_path)
    hadoop_configuration = spark_context._jsc.hadoopConfiguration()
    filesystem = java_path.getFileSystem(hadoop_configuration)

    if filesystem.exists(java_path):
        if not overwrite:
            raise FileExistsError(
                f"Output path already exists: {output_path}. "
                "Use --overwrite to replace it."
            )

        if not filesystem.delete(java_path, True):
            raise RuntimeError(f"Unable to delete output path: {output_path}")


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Creates the command line argument parser.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Compute separate purchase and viewed similarities "
            "between customer pairs with Apache Spark."
        )
    )

    parser.add_argument("input_path")
    parser.add_argument("output_path")

    parser.add_argument(
        "--job1-partitions",
        type=int,
        default=4,
        help="Partitions used while grouping users by item and behavior.",
    )

    parser.add_argument(
        "--job2-partitions",
        type=int,
        default=8,
        help="Partitions used while aggregating user-pair similarities.",
    )

    parser.add_argument(
        "--min-buy",
        type=int,
        default=0,
        help="Minimum common-purchase similarity required in the output.",
    )

    parser.add_argument(
        "--min-pv",
        type=int,
        default=0,
        help="Minimum common-viewed similarity required in the output.",
    )

    parser.add_argument(
        "--max-users-per-item",
        type=int,
        default=-1,
        help=(
            "Discard item/behavior groups above this number of users. "
            "-1 disables the limit."
        ),
    )

    parser.add_argument(
        "--delimiter",
        default=",",
        help="Input column delimiter.",
    )

    parser.add_argument(
        "--user-index",
        type=int,
        default=0,
        help="Zero-based user ID column index.",
    )

    parser.add_argument(
        "--item-index",
        type=int,
        default=1,
        help="Zero-based item ID column index.",
    )

    parser.add_argument(
        "--behavior-index",
        type=int,
        default=2,
        help=(
            "Zero-based behavior column index. "
            "The default is correct after Category ID removal."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete the output directory if it already exists.",
    )

    parser.add_argument(
        "--persist-final",
        action="store_true",
        help=(
            "Persist final pair similarities in MEMORY_AND_DISK before writing. "
            "Useful when --count-results is also enabled."
        ),
    )

    parser.add_argument(
        "--count-results",
        action="store_true",
        help="Count final user pairs and print the result.",
    )

    parser.add_argument(
        "--app-name",
        default="RetailSimilaritySpark",
        help="Spark application name.",
    )

    return parser


def main() -> None:
    """
    Main function of the Spark application.
    """
    args = build_argument_parser().parse_args()

    if args.job1_partitions < 1 or args.job2_partitions < 1:
        raise ValueError("Partition counts must be positive.")

    if args.min_buy < 0 or args.min_pv < 0:
        raise ValueError("Similarity thresholds cannot be negative.")

    spark_conf = SparkConf().setAppName(args.app_name)
    spark_context = SparkContext(conf=spark_conf)
    spark_context.setLogLevel("WARN")

    started_at = time.perf_counter()

    try:
        delete_output_if_needed(
            spark_context,
            args.output_path,
            args.overwrite,
        )

        # Reads the input file as an RDD of lines.
        lines = spark_context.textFile(
            args.input_path,
            minPartitions=args.job1_partitions,
        )

        # Equivalent of Job 1 Mapper:
        # user_id,item_id,behavior,timestamp  -> ((item_id, behavior), user_id)
        parsed = (
            lines
            .map(
                lambda line: parse_record(
                    line,
                    args.delimiter,
                    args.user_index,
                    args.item_index,
                    args.behavior_index,
                )
            )
            .filter(lambda record: record is not None)
        )

        # Equivalent to Job 1 Reducer:
        # ((item_id, behavior), user_id) -> ((item_id, behavior), {distinct users})
        users_by_item_behavior = parsed.combineByKey(
            create_user_set,
            add_user,
            merge_user_sets,
            numPartitions=args.job1_partitions,
        )

        # Removes groups that cannot or should not generate pairs.
        usable_groups = users_by_item_behavior.filter(
            lambda record: item_is_usable(
                record,
                args.max_users_per_item,
            )
        )

        # Equivalent to Hadoop Job 2 Mapper:
        # ((item_id, behavior), {users}) -> ((user1, user2), (1, 0)) for buy
        #                                -> ((user1, user2), (0, 1)) for pv
        pair_contributions = usable_groups.flatMap(
            generate_pair_contributions
        )

        # Equivalent to Job 2 Combiner + Reducer:
        similarities = pair_contributions.reduceByKey(
            add_similarities,
            numPartitions=args.job2_partitions,
        )

        # Equivalent to final threshold filtering in the Hadoop reducer.
        filtered_similarities = similarities.filter(
            lambda record: passes_thresholds(
                record,
                args.min_buy,
                args.min_pv,
            )
        )

        if args.persist_final or args.count_results:
            filtered_similarities.persist(StorageLevel.MEMORY_AND_DISK)

        if args.count_results:
            pair_count = filtered_similarities.count()
            print(f"Final user pairs: {pair_count}")

        (
            filtered_similarities
            .map(format_output)
            .saveAsTextFile(args.output_path)
        )

        elapsed_seconds = time.perf_counter() - started_at

        print(f"Output written to: {args.output_path}")
        print(f"Elapsed time: {elapsed_seconds:.3f} seconds")

    finally:
        spark_context.stop()

if __name__ == "__main__":
    main()
