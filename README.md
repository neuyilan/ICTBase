# ICTBase
基于hbase的coprocessor机制实现了4种维护索引的方式，分别为(sync, async-insert, async-compact, async-simple)。并且实现了其全局二级索引和局部二级索引

具体请参考论文[The Consistency Analysis of Secondary Index on Distributed Ordered Tables](http://ieeexplore.ieee.org/document/7965155/)
