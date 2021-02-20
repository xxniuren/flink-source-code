## 概述  
计数窗口是统计窗口数据的数量，到达一定数量后触发窗口计算。计数窗口分为全局计数窗口和分区计数窗口，计数窗口和时间窗口实现过程
都是一样的，只是计数窗口的window是一个单例无限大的时间窗口，Trigger是CountTrigger，当窗口数据到达一定数量后触发计算函数。分区
计数窗口先对输入数据进行keyBy，然后调用countWindow指定窗口触发器，然后apply或process(process背后也是调用的apply方法)来触发
窗口计算。全局计数窗口是直接countWindowAll指定窗口触发器，然后apply或process来触发窗口计算，countWindowAll方法返回的是AllWindowStream
对象，该对象在构造函数中指定了最大并行度只能为1（countWindowAll只能有一个并行度），且自己构造了keyBy的方法，但是keyBy返回均是0。  

## 使用案例  
### 全局计数窗口  
```java
public class AllWindowCountTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setMaxParallelism(3);
		AtomicInteger cnt = new AtomicInteger(0);
		// 构建输入数据源
		DataStream<Tuple2<String, Long>> inputDataStream = env.addSource(new TimeDataSource()).setParallelism(1);
		// countWindowAll只是构造AllWindowStream对象，AllWindowedStream和DataStream没有任何关系，但是AllWindowStream有apply方法来将输入流构造出SingleOutputStreamOperator对象
		// 所有的窗口操作都最终构造出WindowOperator对象，只是不同的窗口操作的WindowAssigner（窗口分配器），Trigger（触发器）等不一样
		// WindowOperator是OneInputStreamOperator的实现类。
		// input.transform(opName, resultType, operator).forceNonParallel() 在开始的时候就设置了不允许并行计算
		inputDataStream.countWindowAll(2).apply(
				new AllWindowFunction<Tuple2<String, Long>, Object, GlobalWindow>() {
					@Override
					public void apply(
							GlobalWindow window,
							Iterable<Tuple2<String, Long>> values,
							Collector<Object> out) throws Exception {
						//获取数据，并将数据打印输出
						List<String> res = new ArrayList<>();
						cnt.addAndGet(1);
						for (Tuple2<String, Long> val : values) {
							res.add(val.f0 + "@" + val.f1);
						}
						System.out.println(res.toString() + " --------------- " + cnt.toString() + " --------- " + Thread.currentThread().getName());
					}
				});
		env.execute(AllWindowCountTest.class.getSimpleName());
	}
}
```  

### 分区计数窗口  
```java
public class PartitionWindowCountTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		AtomicInteger cnt = new AtomicInteger(0);
		// 构建输入数据源
		DataStream<Tuple2<String, Long>> inputDataStream = env.addSource(new TimeDataSource());
		KeyedStream<Tuple2<String, Long>, String> keyedStream = inputDataStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
			@Override
			public String getKey(Tuple2<String, Long> value) throws Exception {
				return value.f0;
			}
		});
		// keyBy后的countWindow 和 不做keyBy的countWindowAll的区别是 不做keyBy的countWindowAll操作只是在countWindowAll内部做了keyBy，其中
		// keyBy返回的key是0（分发到一个节点上），且限定了countWindowAll的并行度只能为1，其他的没有变化。
		keyedStream.countWindow(2).apply(new WindowFunction<Tuple2<String, Long>, Object, String, GlobalWindow>() {
			@Override
			public void apply(
					String s,
					GlobalWindow window,
					Iterable<Tuple2<String, Long>> input,
					Collector<Object> out) throws Exception {
				//获取数据，并将数据打印输出
				List<String> res = new ArrayList<>();
				cnt.addAndGet(1);
				for (Tuple2<String, Long> val : input) {
					res.add(val.f0 + "@" + val.f1);
				}
				System.out.println(res.toString() + " --------------- " + cnt.toString() + " --------- " + Thread.currentThread().getName());
			}
		});
		env.execute(AllWindowCountTest.class.getSimpleName());
	}
}
```