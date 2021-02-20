## 概述  
全局计数窗口是窗口数据到达设定的个数后触发窗口计算，全局计数窗口只有一个并行度。全局计数窗口编码流程为：inputStream.countWindowAll().apply()  

## 源码实现  
### countWindowAll操作  
```java
//DataStream的AllWindowedStream
public class DataStream {
    // 返回AllWindowStream
	public AllWindowedStream<T, GlobalWindow> countWindowAll(long size) {
		return windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
	}
    
    //WindowAll实现
	public <W extends Window> AllWindowedStream<T, W> windowAll(WindowAssigner<? super T, W> assigner) {
        //构造AllWindowedStream对象，并将WindowAssigner属性注入到该对象中
		return new AllWindowedStream<>(this, assigner);
	}
    
}
```  
在DataStream中定义了countWindowAll方法，返回AllWindowedStream对象。其中WindowAssigner是通过GlobalWindows.create()生成的GlobalWindows对象
该对象是WindowAssigner的子类，其为countWindow操作中的输入数据分配窗口，GlobalWindows的窗口是定义了一个无边界的单例窗口。窗口实现源码如下。  
```java
public class GlobalWindows extends WindowAssigner<Object, GlobalWindow> {
	private static final long serialVersionUID = 1L;
    // 单例对象，定义内部构造方法
	private GlobalWindows() {}
    //返回一个单例的GlobalWindow
	@Override
	public Collection<GlobalWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		return Collections.singletonList(GlobalWindow.get());
	}
}
```  
GlobalWindow的实现代码如下：  
```java
public class GlobalWindow extends Window {
    // 饿汉子单例模式
	private static final GlobalWindow INSTANCE = new GlobalWindow();
	private GlobalWindow() { }
    // 单例对象
	public static GlobalWindow get() {
		return INSTANCE;
	}
    //定义了一个无穷大的时间窗口
	@Override
	public long maxTimestamp() {
		return Long.MAX_VALUE;
	}
}
```  
AllWindowedStream构造函数的源码如下：  
```java
public class AllWindowedStream<T, W extends Window> {
	public AllWindowedStream(DataStream<T> input,
			WindowAssigner<? super T, W> windowAssigner) {
        // 这个地方主动通过NullByteKeySelector做了keyBy，构造成了一个KeyStream，这里主动实现了AllWindowedStream的并行度为1
		this.input = input.keyBy(new NullByteKeySelector<T>()); 
		this.windowAssigner = windowAssigner;
		this.trigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
	}
}
// NullByteKeySelector对象的实现
public class NullByteKeySelector<T> implements KeySelector<T, Byte> {
	private static final long serialVersionUID = 614256539098549020L;
    //只会分发到一个节点上
	@Override
	public Byte getKey(T value) throws Exception {
		return 0;
	}
}
```  
### AllWindowedStream.apply函数  
AllWindowedStream实现了apply方法，apply方法会构造一个WindowOperator对象，WindowOperator对象是OneInputStreamOperator的子类，是窗口核心实现。  
```java
// 窗口操作均使用的 WindowAssigner -> Window -> Trigger -> Evicator -> function  
public class AllWindowedStream<T, W extends Window> {
	public <R> SingleOutputStreamOperator<R> apply(ReduceFunction<T> reduceFunction, AllWindowFunction<T, R, W> function, TypeInformation<R> resultType) {
		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
		}
		//clean the closures
		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;
		String opName;
        //获取keySelector，这里的keySelector是NullByteKeySelector对象，返回全部是0
		KeySelector<T, Byte> keySel = input.getKeySelector();
        // WindowOperator对象
		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableAllWindowFunction<>(new ReduceApplyAllWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
					reduceFunction,
					input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueAllWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}
        // 构造transform，此时设置forceNonParallel，将operator的并行度强制设置为1
		return input.transform(opName, resultType, operator).forceNonParallel();
	}
}
```  
此时构造了WindowOperator对象，该对象是task最终执行的对象，实现源码如下。  
```java
public class WindowOperator {
	// 处理每一条数据
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
        // 为每个元素通过WindowAssigner分配时间窗口，在WindowAll操作中，返回的就是一个无穷大的单例时间窗口
		final Collection<W> elementWindows = windowAssigner.assignWindows( 
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;
        // 获取key
		final K key = this.<K>getKeyedStateBackend().getCurrentKey();
		// todo: 了解一下MergeWindowAssigner的场景，主要是SessionWindow操作时的窗口合并
		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {

						if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
							throw new UnsupportedOperationException("The end timestamp of an " +
									"event-time window cannot become earlier than the current watermark " +
									"by merging. Current watermark: " + internalTimerService.currentWatermark() +
									" window: " + mergeResult);
						} else if (!windowAssigner.isEventTime()) {
							long currentProcessingTime = internalTimerService.currentProcessingTime();
							if (mergeResult.maxTimestamp() <= currentProcessingTime) {
								throw new UnsupportedOperationException("The end timestamp of a " +
									"processing-time window cannot become earlier than the current processing time " +
									"by merging. Current processing time: " + currentProcessingTime +
									" window: " + mergeResult);
							}
						}

						triggerContext.key = key;
						triggerContext.window = mergeResult;

						triggerContext.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							triggerContext.window = m;
							triggerContext.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
					}
				});

				// drop if the window is already late
				if (isWindowLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}
				isSkippedElement = false;

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				windowState.setCurrentNamespace(stateWindow);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = actualWindow;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(actualWindow, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			for (W window: elementWindows) {
				// 判断窗口是否延迟，如果延迟则丢弃，只是在事件时间窗口会做判断
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;
				// 窗口中间数据存储到状态缓存中，todo：查看一下ListState状态缓存结构
				windowState.setCurrentNamespace(window);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = window;
				// 在countWindow操作时，构造的是CountTrigger对象，该对象会持续计算当前输入的数据，然后判断数据达到预设值后将TriggerResult设置为Fire
				TriggerResult triggerResult = triggerContext.onElement(element);
				// 处理时间均返回的Continue信息
				if (triggerResult.isFire()) {
				    // 将窗口中的数据获取出来，然后在emitWindowContents对象中，利用用户定义的Function进行计算
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(window, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(window);
			}
		}
		// 对哪些跳过的数据或延迟数据如果设定了旁路输出，则放到旁路输出中
		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if (isSkippedElement && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}
    /**
     * 发送窗口数据，这里调用apply中定义的userFunctions.process方法
    **/
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		processContext.window = window;
        // process方法最后调用的是AllWindowFunction的apply的实现逻辑 
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}
}
```
CountTrigger对象的实现源码  
```java
public class CountTrigger<W extends Window> extends Trigger<Object, W> {
	@Override
	public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
		ReducingState<Long> count = ctx.getPartitionedState(stateDesc); // count值写入到了TriggerContext中
		count.add(1L);
		if (count.get() >= maxCount) {
			count.clear(); // 如果count大于等于设定的阈值，则开启触发
			return TriggerResult.FIRE;
		}
		return TriggerResult.CONTINUE;
	}
    // 不做处理
	@Override
	public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
		return TriggerResult.CONTINUE;
	}
    // 不做处理
	@Override
	public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}
    // 清理分区状态，todo：看一下TriggerContext对象的使用
	@Override
	public void clear(W window, TriggerContext ctx) throws Exception {
		ctx.getPartitionedState(stateDesc).clear();
	}
}
```






## 参考代码  
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
    // 注释：该transform中的依赖链路是 SourceTransformation -> PartitionTransformation -> OneInputTransformation 由于此处没有定义sink 所以没有sinkTransformation  
    // PartitionTransformation会生成一个VirtualPartitionNode附着在Source节点上。

}
```  


