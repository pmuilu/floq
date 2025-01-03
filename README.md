# Transform Real-Time Data Streams with Floq

Floq is an experimental data-parallel streaming framework designed for AI agents and lightweight tasks that don’t require the complexity of large-scale distributed systems like Apache Flink, Apache Spark, or Apache Beam with Cloud Dataflow.

With efficient CPUs readily available, many socket streams can be processed effectively using minimal computational resources. Floq is designed for such cases, providing a straightforward and efficient approach to real-time data processing.

See examples for more details, for example [count_words](examples/count_words/src/main.rs) example combines Mastodon and Bluesky firehoses into a single stream and counts words in each window of 10 seconds.