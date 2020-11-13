mvn package && \
spark-submit \
    --class xyz.jonaylor.KNN \
    --master local \
    target/kNN-2.0.jar \
    datasets/Train/small.csv \
    5