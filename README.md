## LNG 翻滚事件模拟及检测
### 模拟数据
实验模拟船只上不同传感器的数值，目的是为了检测翻滚事件，并在检测到翻滚事件的时候，对其产生的原因和事情发生的相位进行溯源，检测的数据主要有：
1. 船只上不同储物罐温度传感器的数据，在数据模拟中使用了正常流和异常流，模拟原理是定时产生异常流，将异常流与正常流进行合流，当检测到储物罐A的传感器c正在生成异常数据时，忽视c的正常数据源；
2. 船只加注数据，LNG抽取数据，主要是为了模拟由于不同地区LNG产生的分层；
3. 船只翻滚事件的报警。
4. 罐柜压力数据。

### 事件检测
由原始数据识别Simple Event
在 rollover 这个例子中识别 layer （分层） 和 overpressure（超压）事件 

当同时检测到分层和超压在10s的事件窗口中，我们认为翻滚事件已经发生

这个时候我们根据识别的简单事件中的layer的原始数据在Kafka中的Offset 和 overpressure在Kafka中的Offset，利用Kafka对数据的缓存和offset重放机制，对数据进行回溯

对回溯的数据进行封装，生成CE，存储在Kafka中。

### 事件溯源
当检测到roll-over事件时，我们现有的背景知识，rollover发生的必然条件是分层，导致分层的原因有两种：
1. 两种不同地区，不同氮含量的LNG混合产生的分层；
2. 船只运输过程中摇曳和热传递产生的分层；

当我们检测到rollover发生时，探索器对Bunkeringlog进行重新消费，维护一个由加注产生的分层的Layer List，寻找当前rollover发生时，发生的分层是在第几层，该分层是否由加注产生。








