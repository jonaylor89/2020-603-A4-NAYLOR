mvn package && \
spark-submit \
    --class xyz.jonaylor.KNN \
    --master local \
    target/kNN-2.0.jar \
    datasets/small.csv \
    5
