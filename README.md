# Dataset Preprocessing

The original `UserBehavior.csv` dataset is almost 4 GB. For local development and testing, we created a reduced version with a target size between **1.50 GB and 1.60 GB**.

The preprocessing script:

- removes the unused `Category ID` column;
- selects complete user profiles instead of random rows;
- keeps mostly regular users;
- adds users who share `buy` or `fav` items with the selected users;
- preserves a small percentage of low-activity and high-activity users.

Sampling complete users avoids breaking their interaction history and helps preserve meaningful similarities. The inclusion of a few outliers also makes the reduced dataset more realistic.

Repeated events are not removed during preprocessing. This is intentional because Hadoop Job 1 is responsible for deduplicating users for each `(item_id, behavior)` pair:

```text
(item_id, behavior) -> distinct user IDs
J
ob 2 is not affected either. It still generates user pairs and computes two separate values:

(user1, user2) -> (common_buy_items, common_favorite_items)

The reduced dataset represents a smaller population, but the logic and correctness of both Hadoop jobs remain unchanged.