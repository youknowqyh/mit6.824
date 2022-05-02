### Lab 1: MapReduce

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


### Lab 2: Raft

#### Intro

很早以前，人们有构造多副本系统的需求，但是却面临脑裂（split brain）的问题。

后来人们发现，避免脑裂的关键是过半票决（Majority Vote）。

Raft: a consensus algorithm, a replicated state machine protocol

首先要看，[实验handout](http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html)

以下两个Raft可视化网站对于理解Raft太有帮助了！：

[http://thesecretlivesofdata.com/raft/](http://thesecretlivesofdata.com/raft/)

[https://raft.github.io/](https://raft.github.io/)

这个[学生指南](https://thesquareplanet.com/blog/students-guide-to-raft/)也很有用！

[TA教你debug](https://youtu.be/UzzcUS2OHqo?t=3629)太有用了，没思路可以看看他的代码。。。这应该不算抄吧，官方给出来的诶！

[Part2A没思路可以看的好东西](http://nil.csail.mit.edu/6.824/2020/labs/raft-structure.txt)

实验目标：

- implement Raft as a Go object type with associated methods
- Raft instances talk to each other with RPC to maintain replicated logs.

这个实验主要实现论文的section5，论文的section6不用看，因为不用实现。Lab 3实现section7；

#### Part 2A: leader election, heartbeats

##### 原理

在Raft中有两种timeout来控制election：`election timeout`和`heartbeat timeout`。

`election timeout`(randomly chosen from 150ms~300ms to avoid split vote): 一个follower成为candidate需要等待的时间。

当一个成为candidate后，它会开始一个新的election term，并且将vote count置为1（自己给自己投票），发送`RequestVote`消息给别的节点。如果接受到消息的节点在这个term还没有投过票，就会投给这个candidate，并且将timeout重置。

如果一个candidate拿到过半投票，它会成为leader。它会开始发送`AppendEntries`消息给它的follower。（这时别的服务器就会隐晦地通过接收特定任期号的heartbeat来知道，选举成功了）

发送`AppendEntries`的间隔被称为`heartbeat timeout`。

Followers会对`AppendEntries`消息做出回应，重置election timeout，将term设置为leader的term。

这个election term会一直持续，直到它不能够再接受到`heartbeat`。

如果一个follower在election timeout这段时间内都没收到heartbeat，那么它自己就会成为candidate，然后开始新的选举（currentTerm + 1）（会随机选择一个新的election timeout）

不管是在处理RPC请求，或是收到了RPC的响应中的term大于currentTerm，都会把自身变为Follower，然后reset the election timer。

##### 可能出现的问题：split vote

如果两个followers同时成为了candidates，那么split vote就有可能发生。

split vote时，所有节点会等待各自的timeout完毕后来进行新一轮的election。

起初，作者想设计一个ranking system来解决这个问题，后来发现randomized retry approach更好！

##### 注意点

> In particular, many would simply reset their election timer when they received a heartbeat, and then return success, without performing any of the checks specified in Figure 2. 

所以收到heartbeat不仅仅要reset timeout，还要check一些别的东西？！

Specifically, you should only restart your election timer if a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer); b) you are starting an election; or c) you grant a vote to another peer.

network partition
leader收到更高term的消息后会主动卸任

##### 实现

The management of the election timeout is a common source of
headaches. 
Perhaps the simplest plan is to `maintain a variable` in the
Raft struct containing the `last time` at which the peer heard from the
leader, and to have the election timeout goroutine periodically check
to see whether the `time since then` is greater than `the timeout period`.
It's easiest to use `time.Sleep()` with a small constant argument to
drive the periodic checks. Don't use time.Ticker and time.Timer;
they are tricky to use correctly.

heartbeats -> `AppendEntries` RPCs with no log entries

需要设计一个结构来存储log entry。

The election timeout is randomized to be between 150ms and 300ms.

Specifically, you should only restart your election timer if a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer); b) you are starting an election; or c) you grant a vote to another peer.

Raft instances之间只能通过`RPC`交互，别的方法都不被允许。

##### 测试：`go test -run 2A` `go test -race -run 2A`


#### Part 2B: log replication

主要实现`Start`函数，实现`log replication`。还要实现论文5.4.1中的`election restriction`（但这个我好像在Part A已经实现了，所以主要要实现的就是log replication）

election restriction证明：
election restriction是指，一个candidate选举时，只有当它的最后一个entry的term比别的节点的最后一个entry的term大，别的节点才会给它投票。因为`如果一个candidate的最后一个节点的term比过半的节点的最后一个entry的term大，它包含所有committed的logEntries`。

换句话说，这个candidate如果想要当选，它的最后一个节点的term肯定比过半的节点的最后一个entry的term大。

所以，这个candidate A包含了所有committed的entry。

有没有可能有一个别的节点包含A没有的committed的logEntry？

可以用反证法来证明`如果一个candidate的最后一个节点的term比过半的节点的最后一个entry的term大，它包含所有committed的logEntries`：

假设candidate A最新的logEntry的term为x

假设存在这样一个节点，它包含比A还多的committed entries，那么至少超过一大半的节点包含logEntry（term > x）

这样的话，candidate A不满足`它的最后一个节点的term比过半的节点的最后一个entry的term大`

所以相矛盾。所以原推论是正确的。

##### 原理

make log consistent across our cluster...

一旦我们选举出一个leader后，我们需要将所有的系统改动都复制到所有节点中（通过AppendEntries）。

首先，client将变化发送给leader，这个变动会被添加到leader的log中。

接下来，这个变化会在下一次heartbeat中发送给所有followers。

当一个logEntry被超过一半的节点ACK，它就会对client做出响应。

Raft可以在发生网络分区（network partition）也保证正常运行。

再网络分区恢复后，之前被分开的A和B会同时回滚它们uncommited的entries，然后匹配新的leader的log。


`rf.Start(command)->(index, term, isleader)`KV层会调用这个方法，然后对Raft说：嘿，Raft，我接受了用户的这个请求，请把它存在log中，并在commited之后告诉我！（Start会立刻返回，不需要等待log被添加。）

等Raft commit了这个log后，Raft会通过`ApplyCh`告诉KV层`ApplyMsg`：哈，你刚刚在`rf.Start`函数中传给我的请求已经commit了。（Note: Raft通知的不一定是最近一次Start函数传入的请求；）

index # ---> log entry (once commited, send the log entry to the larger service, e.g. k/v store, to execute)

AppendEntries会发送prevLogIndex, prevLogTerm和entries[]，在要添加的entries的前一个槽的信息应该与prevLogIndex, prevLogTerm一致。

leader会维护一个`nextIndex[]`数组，它记录了要发送给每个follower的第一个log的index，nextIndex越小，`entries[]`的size越大。一开始entries为空，因为nextIndex指向的是leader的第一个空槽。

##### 实现

If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log, you should handle it the same as if you did have that entry but the term did not match (i.e., reply false).

Check 2 for the AppendEntries RPC handler should be executed even if the leader didn’t send any entries.

Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.

matchIndex is initialized to -1 (i.e., we agree on no prefix)

如果`commitIndex > lastApplied`，在`AppendEntries` RPC handler中进行处理，ensure that application is only done by one entity.

当一个follower允许了AppendEntries时，matchIndex会更新，nextIndex也会更新。

当收到RPC回复时，不要这样去更新 matchIndex：` matchIndex = nextIndex - 1, or matchIndex = len(log) `。因为这两个值都很有可能已经被修改了。应该通过`prevLogIndex + len(entries)`去update。

不管heartbeat中携带的logEntries是否为空，都要一视同仁!!!! 主要是通过PrevLogIndex和PrevLogTerm来进行匹配和复制的，而不要用entries是否为空来作为条件去执行对应的逻辑。

实际实现中还碰到了很多的坑，比如说如果PrevLogIndex和 PrevLogTerm已经对齐了，但是很有可能follower后面的元素还没有truncate掉，所以再进行append的时候要再做一次truncate。

反正test-driven development，没有测试用例根本不可能写出来啊！！！设计项目的人是怎么想到这些测试用例的啊。



#### Part 2C: persistency

为什么commitIndex和lastApplied可以是volatile的呢？？？

因为机器崩溃了之后，重启程序后，肯定需要重跑，再把数据apply到kv server上去。
Part 2C应该会涉及到这部分，先把这个问题记录下来。

```
For simplicity, you should save Raft’s persistent state just after any change to that state. The most important thing is that you save persistent state before you make it possible for anything else to observe the new state, i.e., before you send an RPC, reply to an RPC, return from Start(), or apply a command to the state machine.

If a server changes persistent state, but then crashes before it gets the chance to save it, that’s fine – it’s as if the crash happened before the state was changed. However, if the server changes persistent state, makes it visible, and then crashes before it saves it, that’s not fine – forgetting that persistent state may cause it to violate protocol invariants (for example, it could vote for two different candidates in the same term if it forgot votedFor).
```

TestFigure8Unreliable2C, TestReliableChurn2C过不了啊啊啊啊啊啊，这两个case太难了。得优化log backtracking.

参考：MIT6.824 Lab2 raft中遇到的问题该怎么解决？ - Csomnia的回答 - 知乎
https://www.zhihu.com/question/63895944/answer/713481675

https://yuerblog.cc/2020/08/16/mit-6-824-distributed-systems-%E5%AE%9E%E7%8E%B0raft-lab2c/

https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations

https://zhuanlan.zhihu.com/p/464701798

config.go:475: one(7417056184806773601) failed to reach agreement这个就是时间太久了，要实现优化：

```
You will probably need the optimization that backs up nextIndex by more than one entry at a time. Look at the extended Raft paper starting at the bottom of page 7 and top of page 8 (marked by a gray line). The paper is vague about the details; you will need to fill in the gaps, perhaps with the help of the 6.824 Raft lectures.
```

#### Lab 2总结

为了管理electionTimer，可以让每个raft实例维护一个变量timerStartTime，然后一个main loop每隔一段时间去check一下running time是否超出election timeout了。

currentTerm太重要了，Raft实例就是通过他和RPC中传递的Term实现Leader electiond，可以保证只有一个Leader，因为他可以使得节点有信息来进行状态变化，比如说old leader变成follower。

要实现leader election，正确的在leader crash后重新选出新的leader，还要保证选出来的leader包含所有committed的logEntries(`election restriction`，其实就是比较candidate和follower最新entry的term)。

然后要实现log replication，appendEntries会发送prevLogIndex, prevLogTerm和entries[]，在要添加的entries的前一个槽的信息应该与prevLogIndex, prevLogTerm一致。

leader会维护一个`nextIndex[]`数组，它记录了要发送给每个follower的第一个log的index，nextIndex越小，`entries[]`的size越大。一开始entries为空，因为nextIndex指向的是leader的第一个空槽。

不管heartbeat中携带的logEntries是否为空，都要一视同仁!!!! 主要是通过PrevLogIndex和PrevLogTerm来进行匹配和复制的，而不要用entries是否为空来作为条件去执行对应的逻辑。

实际实现中还碰到了很多的坑，比如说如果PrevLogIndex和 PrevLogTerm已经对齐了，但是很有可能follower后面的元素还没有truncate掉，所以再进行append的时候要再做一次truncate。

反正test-driven development，没有测试用例根本不可能写出来啊！！！设计项目的人是怎么想到这些测试用例的啊。

TestReliableChurn2C和TestUnreliableChurn2C有时候过不了。。累了，不改了，就这样吧。

我还是太菜了呀。:(

```
yuhaoq@yuhaoqdeMacBook-Pro ~/yuhaoq/courses/mit6.824/6.824/src/raft % go test -run 2A
Test (2A): initial election ...
  ... Passed --   3.1  3   60   17256    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  144   28601    0
PASS
ok      _/Users/yuhaoq/yuhaoq/courses/mit6.824/6.824/src/raft   7.674s
```


```
yuhaoq@yuhaoqdeMacBook-Pro ~/yuhaoq/courses/mit6.824/6.824/src/raft % go test -run 2B
Test (2B): basic agreement ...
  ... Passed --   0.9  3   16    4494    3
Test (2B): RPC byte count ...
  ... Passed --   2.4  3   50  114680   11
Test (2B): agreement despite follower disconnection ...
  ... Passed --   6.1  3  134   34995    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.5  5  252   47030    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.8  3   16    4618    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   4.0  3  140   32880    3
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  26.7  5 2424 1834896  103
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.1  3   42   12426   12
PASS
ok      _/Users/yuhaoq/yuhaoq/courses/mit6.824/6.824/src/raft   46.613s
```


```
yuhaoq@yuhaoqdeMacBook-Pro ~/yuhaoq/courses/mit6.824/6.824/src/raft % go test -run 2C
Test (2C): basic persistence ...
  ... Passed --   5.5  3  124   33254    7
Test (2C): more persistence ...
  ... Passed --  16.3  5 1064  216884   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.8  3   36    8997    4
Test (2C): Figure 8 ...
  ... Passed --  29.9  5 1160  243003   39
Test (2C): unreliable agreement ...
  ... Passed --  18.3  5  780  230242  256
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  19.2  5 1916 1868999  227
Test (2C): churn ...
  ... Passed --  16.5  5  796  418912  125
Test (2C): unreliable churn ...
  ... Passed --  16.2  5  704  296330  125
PASS
ok      _/Users/yuhaoq/yuhaoq/courses/mit6.824/6.824/src/raft   123.902s
```

不得不说 electionTimeout 的重置一定要各种小心。在 TestPersist22C 中遇到一个这个错误，原因之前每次转为 Follower 就重置 ElectionTimer，实际上应当只有在 Grant a Vote 时重制 timer，收到更高 Term 的消息时会转为 Follower 但不重置 Timer。

Students' Guide 中强调了好几次 Election Timer 的重要性，千千万万不要乱设置：

Make sure you reset your election timer exactly when Figure 2 says you should.

However, if you read Figure 2 carefully, it says：If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate.

https://zhuanlan.zhihu.com/p/268647741

### Lab 3: 

build a key/value service on top of Raft

两个部分，key/value service 和 log compaction

Clerk不知道哪个kvserver是Raft Leader，如果Clerk请求给错误的Server或者无法请求，Clerk会重新发送请求给一个不同的kvserver。

如果kv service进行了对该commit操作，leader会作出响应。

但如果commit失败了（leader被替换了），server会报错，Clerk会进行retry

当Raft提交commit后，server要执行`Op` command。

只有当server是part of a majority时，才能执行Get()，

Clerk多次发送RPC multiple times，要保证re-send doesn't result in the servers executing the request twice.

> This means that your client-facing RPC methods shuold simply submit the client's operation to Raft, and then wait for that operation to be applied by this "applier loop"

如果client发送`APPEND`请求到服务器，但是没有得到response，他会重新发送给另一台server，server需要确定APPEND操作没有被执行两次。Solution: unique identifier for each client request so that you can recognize if you have seen, and more importantly, applied, a particular operation in the past. This state needs to be a part of your state machine so that all your Raft servers eliminate the same duplicates. One simple way and fairly efficient one is to give each client a unique identifier, and then have them tag each request with a monotonically increasing sequence number. 

each client has a unique identifier 
each request with a monotonically increasing seqeuence number
server keeps tracks of the latest sequence number if has seen for each client, and simply ignores any operation that it has already seen.

艹，开始写的有点弄不懂他是怎么build up connection的。想了一会发现其实server和application server没关系啊。比如说有5台服务器，就先通过RPC register这五台。

每个server上分别运行一个kv server application，每个kv server和client都可以通过rpc请求每台服务器。

可以参考kv.go，单机版的。


Client启动时，会随机选择一个server作为leader
如果Clerk请求的server不是leader，该server会拒绝请求，然后告诉Clerk谁是leader
如果请求超时了，Clerk会重新选择一个server作为leader

为了避免duplication request，每个client都有个自己的unique identifier，（the state machine tracks the latest serial number processed for each client, along with the associated response).. 

我的问题是，server保存着每个client最新的sequence number，但这个server崩溃了之后，别的server成为leader后，别的server怎么知道有没有执行过这个命令呢？

把这个信息也存在raft里？感觉不是这么搞的。raft应该只存放log吧。
把他作为volatile state如何 0.0 感觉可以这么搞。

linearizable reads must not return stale data. Raft handles this by having each leader commit a blank no-op entry into the log at the start of its term



摆烂了，直接开抄吧。感觉自己的脑子想不出来。

https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab2.md



> Another issue many had (often immediately after fixing the issue above), was that, upon receiving a heartbeat, they would truncate the follower’s log following prevLogIndex, and then append any entries included in the AppendEntries arguments. This is also not correct. We can once again turn to Figure 2:
> 许多人遇到的另一个问题（通常是在修复了上述问题之后）是，在收到心跳时，他们会在prevLogIndex之后截断跟随者的日志，然后追加AppendEntries参数中包括的任何条目。这也是不正确的。我们可以再一次转向图2：
> 这里的if很关键。如果跟随者拥有领导者发送的所有条目，跟随者必须不截断其日志。任何跟随领导者发送的条目的元素都必须被保留。这是因为我们可能从领导者那里收到了一个过时的AppendEntries RPC，而截断日志将意味着 "收回 "我们可能已经告诉领导者我们的日志中的条目。

我艹。怪不得直接错了。。。

然后记得一开始加一个空的entry，不然会被边界判断搞到崩溃的。


# Summary 

总结一下为啥我的raft有问题

首先candidate在请求vote得时候，每个vote协程拿到锁后应该再判断一下是否是Candidate并且是否和args中的Term一致。


Replicate的时候在拿到锁后也要判断状态是否是Leader。

通过log consistency check后，在做merge的时候，不应该直接truncate然后append，

有三种情况：

-------
    ------

-------
       ------

-------------- (outdated AppendEntries, we should keep all the records the follower has accepted from the leader)
  -----


应该从左往后遍历entries，直到遍历完，或找到一个term不一致的entry，把它作为分界点，将rf.log在此之后的entries都删掉，然后将剩下的append进去。


总算找到错误了tmd