SimpleCQL
=========
SimpleCQL is an extension to SimpleDB that introduces continuous query semantics from CQL. SimpleCQL supports a wide range of complex SQL-like queries over continuous, unbounded streams of data.  The versatility of SimpleCQL is illustrated through three primary examples -- aggregation of key statistics from real-time error logs, computation of trends of real-time tweets from Twitter, and computation of real-time advertisement statistics.  SimpleCQL is measured against these examples, while other stream data management systems use the Linear Road benchmark.  It is important to note that minor adjustments render SimpleCQL capable of supporting the Linear Road benchmark.

We evaluate SimpleCQL and show that its performance is on par with most stream-processing systems.  SimpleCQL achieves real-time processing speeds of up to 400k tuples/second based on our benchmarks.  Furthermore, this system allows for the processing of complex queries against streaming data.

The full paper can be found [here](./paper/SimpleCQL.pdf).
