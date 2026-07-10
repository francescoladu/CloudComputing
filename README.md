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

Creaiamo le cartelle sulla VM 

ssh root@10.1.1.112 'mkdir -p /root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/job1'
ssh root@10.1.1.112 'mkdir -p /root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/job2'
ssh root@10.1.1.112 'mkdir -p /root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/writable'
ssh root@10.1.1.112 'mkdir -p /root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity'
ssh root@10.1.1.112 'mkdir -p /root/retail_similarity/spark'
ssh root@10.1.1.112 'mkdir -p /root/retail_similarity/dataset'


E Copiamo tutti i files 
<scp *.java root@10.1.1.112:/root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/job1/>

cd ../job2
scp *.java root@10.1.1.112:/root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/job2/

cd ../writable
scp *.java root@10.1.1.112:/root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/writable/

cd ..
scp RetailSimilarityDriver.java root@10.1.1.112:/root/retail_similarity/hadoop-retail/src/main/java/retailsimilarity/

scp Spark/RetailSimilarity.py root@10.1.1.112:/root/retail_similarity/spark/

scp UserBehaviorModified.csv root@10.1.1.112:/root/retail_similarity/dataset/


TREE FINALE 
/root/retail_similarity/
├── dataset
│   └── UserBehaviorModified.csv
├── hadoop-retail
│   └── src/main/java/retailsimilarity
│       ├── RetailSimilarityDriver.java
│       ├── job1
│       │   ├── BehaviorInversionMapper.java
│       │   └── DistinctUsersReducer.java
│       ├── job2
│       │   ├── PairGenerationMapper.java
│       │   ├── SimilarityCombiner.java
│       │   └── SimilarityReducer.java
│       └── writable
│           ├── ItemBehaviorWritable.java
│           ├── SimilarityWritable.java
│           ├── UserListWritable.java
│           └── UserPairWritable.java
└── spark
    └── RetailSimilarity.py

ovviamente abbiamo caricato il pom.xml

e compiliamo 

mvn clean package -DskipTests

procediamo a creare le cartelle HDFS 

hdfs dfs -mkdir -p /retail/input
hdfs dfs -mkdir -p /retail/output
hdfs dfs -mkdir -p /retail/intermediate


e a caricare il dataset

hdfs dfs -put -f dataset/UserBehaviorModified.csv /retail/input/

e a verificare che sia tutto ok

hdfs dfs -ls -h /retail/input
hdfs dfs -du -h /retail/input/UserBehaviorModified.csv


