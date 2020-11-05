# 基于Hadoop MapReduce 的 PageRank 算法实现

> 编译和运行：`gradle clean; gradle build; hadoop jar pagerank/build/libs/pagerank.jar hadoop/Main`

## PageRank 算法介绍

​		PageRank算法是Google的创始人Page等人于1998年提出的。该算法是基于Web超链接结构分析的算法，并通过链接结构计算网页的重要程度，是评鉴网页排名的重要工具。
​		PageRank算法利用网络结构中的反向链接信息对网页进行排序，是一种与主题无关的排序算法。其主要思想是基于网页链接结构的分析，即一个网页的质量和重要性是通过指向它的网页数量来衡量，数量越多越重要；另一方面，指向它的网页的重要性（PageRank值）越高，该网页越重要。

​		其算法思想可用以下公式表述：

<img src="http://skyler-pics.oss-cn-beijing.aliyuncs.com//uPic/2020/%7Bmon%7D/05/image-20201105163204072.png" alt="image-20201105163204072" style="zoom: 50%;" />

## 数据集分析与处理

​		源数据格式为起始网页+\t+用逗号,分割的起始网页所连接指向的其它网页列表，eg:A B,D,F......
所以考虑首先读入一遍全体数据，先初始化每个网页的权重为1.0，并在往后的每一次迭代中更新这个记录每个网页权重pr值，每次将上一轮更新后的pr值表传给下一次迭代计算新的pr值。
具体在map时将每一行源数据拆分，首先读取到A的pr值，而后统计A后一共的出链数目num,将A指向的每个网页名作为一个key，value设为pr/num,表示A对该网页的贡献量，如上例中的一行数据经 过mapper后将得到的是：<B,prA/num>、<D,prA/num>、<F，prA/num>......
而后在reducer端接受到的将是同一个key为网页，value是其它所有网页对它pr值贡献量的值列表，eg:<B, <0.1 , 0.2 , 0.6 ......>>,所以reducer要做的便是利用公式将列表中值相加得到sum值，而后将sum乘以权重d再加上(1-d)便得到更新后的网页B的pr值：



## PageRank 算法实现

### 算法步骤

### 数据共享

### 健壮性分析











