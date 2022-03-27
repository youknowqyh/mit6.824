### Lab 1 MapReduce

Paper肯定得看。不过不用太抠细节。

再把[这个](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html
)多看几遍。很重要！

只要捋清楚Map Reduce的过程，这个实验就特别简单了

`M: # of Map Tasks`

`R: # of Reduce Tasks`

`# Workers`取决已你开了多少个Worker进程

这个实验略过了Split的过程，我们已经知道了有多少个Splits。

所以M=`# of splits`

MapPhase：读取一个split，调用mapf，将每一个kv pair根据(hash(k) % R)存放到`mr-MapTaskID-ReduceTaskID`文件中。

执行完MapPhase后，应该有M*R个intermediate files

ReducePhase：根据该Task对应的ReduceTaskID，读取相应的M个intermediate files，然后调用reducef，将结果输出到mr-ReduceTaskID中即可。