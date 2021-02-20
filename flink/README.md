# 编程步骤    
1. 构建ExecutionEnvironment执行环境；  
2. 构建Source数据；
3. 构建转换关系，构建各种转换function；
4. 构建SinkFunction；  
5. 实际执行过程；  
```java
public class SocketTextStreamWordCount {

	public static void main(String[] args) throws Exception {
		//构建ExecutionEnvironment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//读取输入数据
		//生成textStream，没有放在env的transforms中
		DataStream<String> textStream = env.fromElements(WordCountData.WORDS);
		//数据转换
		//生成flatMapStream，并指定了父Task为textStream，并把flatMapStream放到transforms中
		SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = textStream
				.flatMap(new LineSplit())
				.setParallelism(1);
		//生成keyedStream并定义了KeySelector
		KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMapStream.keyBy(0);
		//生成SumAggregator，并将SumAggregator放到env的transforms中
		DataStream<Tuple2<String, Integer>> sum = keyedStream
				.sum(1)
				.setParallelism(2);
		//结果打印输出
		//将PrintSinkFunction加入到transforms中
		sum.print();
		//执行代码
		env.execute(SocketTextStreamWordCount.class.getSimpleName());
	}

	/**
	 * FlatMap继承方法，对每行进行切割
	 */
	public static final class LineSplit implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			// 需要明白如何处理到这个类逻辑的，逻辑应该是序列化过。
			String[] splitedValues = value.split("\\W+");
			for (String splitedValue : splitedValues) {
				out.collect(new Tuple2<>(splitedValue, 1));
			}
		}
	}
}
```  

# 整体执行过程  

# 版本信息 Flink 1.10


# 部署区别  
Flink提供在Yarn上两种运行模式：Session-Cluster和Per-Job-Cluster，其中Session-Cluster的资源在启动集群时就定义完成，后续所有作业的提交都共享该资源，
作业可能会互相影响，因此比较适合小规模短时间运行的作业，对于Per-Job-Cluster而言，所有作业的提交都是单独的集群，作业之间的运行不受影响（可能会共享CPU计算资源），
因此比较适合大规模长时间运行的作业。