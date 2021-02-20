# 事件时间窗口工作过程  
## 概述  
事件时间窗口和处理时间窗口基本类似也是将输入流按照用户重写的时间提取拆分成多个时间窗口，数据通过windowAssigner分配到不同的
窗口下，并注册窗口定时器，在指定时间内触发窗口操作；不同点是（source算子后生成watermark，不是source阶段生成watermark）watermark的生成
也会作为一个operator执行；     
![窗口分配到触发过程](../pic/窗口分配到触发过程.png)   

## 基础概念  
### 时间  
时间分为处理时间（flink处理该数据的时间，和本地时钟有关系）、事件时间（StreamRecord本身的时间）和摄入时间    

### 窗口  
窗口分为滑动窗口、滚动窗口和会话窗口，Window的实现类有TimeWindow和GlobalWindow，TimeWindow是时间窗口，GlobalWindow是
全局窗口；  
```java
public class TimeWindow extends Window {
    // 时间窗口的起始和结束时间属性    
	private final long start;
	private final long end;
	public TimeWindow(long start, long end) {
		this.start = start;
		this.end = end;
	}
}
```     
 
### 窗口分配器  
WindowAssigner是窗口分配器，其定义了窗口的分配规则和默认的触发器等，例如滚动事件时间窗口，其定义了窗口分配规则，    
窗口计算公式：    

```java
timestamp - (timestamp - offset + windowSize) % windowSize;
```  

### 触发器  
触发器定义了触发条件，触发器的申明在keyStream.timeWindow中定义的，触发器的重点实现类有滚动处理时间窗口触发和滚动事件时间窗口触发，

## 代码实现  
### WaterMark生成逻辑    
```java
//inputStream.assignTimestampsAndWatermarks 周期性watermark生成逻辑   
public class TimestampsAndPeriodicWatermarksOperator<T>
		extends AbstractUdfStreamOperator<T, AssignerWithPeriodicWatermarks<T>>
		implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {
    // 处理每个元素，步骤：1. 用户自定义方法提取timestamp；2. 将StreamRecord的hasTimestamp设置为True，且设置其timestamp
    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
                element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);
        // 做timestamp替换
        output.collect(element.replace(element.getValue(), newTimestamp));
    }
    // 执行回调方法，发送WaterMark，持续周期性的WaterMark生成器
    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        // register next timer
        Watermark newWatermark = userFunction.getCurrentWatermark();  // 通过用户自定义方法获取WaterMark信息
        if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {	// 如果newWaterMark大于当前WaterMark，则发送WaterMark信息
            currentWatermark = newWatermark.getTimestamp();
            // emit watermark
            output.emitWatermark(newWatermark);
        }
        // 再次注册定时器，等待下一次发送WaterMark信息，可以通过watermarkInterval来决定watermark生成频率  
        long now = getProcessingTimeService().getCurrentProcessingTime();
        getProcessingTimeService().registerTimer(now + watermarkInterval, this); // 这个是由程序驱动的，不能完全叫做定期执行
    }
    // 在流任务关闭时（批处理场景），此时TimestampsAndPeriodicWatermarksOperator会发出无穷大的WaterMark信息来终止整个工作流  
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}
}
```  

### WaterMark传递逻辑    
WaterMark会作为一种StreamElement在Flink内部流动，当检测到是WaterMark信息时，系统会判断当前所有通道的WaterMark的最小值，并往下进行传递；  
```java
public final class StreamTaskNetworkInput<T> implements StreamTaskInput<T> {

	/**
	 * 处理每一个元素，WaterMark也被封装成了StreamElement，在进行传递
	 *
	 */
	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
        // 如果streamElement是record的话，那么处理记录
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) { // watermark也是StreamElement对象
            // 如果是watermark，那么处理WaterMark信息，通过statusWaterMarkValue查找出所有通道中最小的WaterMark并输出  
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}    
}
```
处理WaterMark过程，当接收到WaterMark的StreamElement时，先查看输入所有通道的最小WaterMark，如果有WaterMark相关定时器的定义，那么
获取每一个满足的定时器（定时窗口的endTime小于WaterMark值），并触发onEventTime方法。
```java
public class StatusWatermarkValve {
    //数据输入通道
	private final InputChannelStatus[] channelStatuses;
	// 最近输出的WaterMark信息
	/** The last watermark emitted from the valve. */
	private long lastOutputWatermark;
    //最近的WaterMark状态
	/** The last stream status emitted from the valve. */
	private StreamStatus lastOutputStreamStatus;   

	public void inputWatermark(Watermark watermark, int channelIndex) throws Exception {
		// ignore the input watermark if its input channel, or all input channels are idle (i.e. overall the valve is idle).
		if (lastOutputStreamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isActive()) {
			long watermarkMillis = watermark.getTimestamp();

			// if the input watermark's value is less than the last received watermark for its input channel, ignore it also.
			if (watermarkMillis > channelStatuses[channelIndex].watermark) {
				channelStatuses[channelIndex].watermark = watermarkMillis;

				// previously unaligned input channels are now aligned if its watermark has caught up
				if (!channelStatuses[channelIndex].isWatermarkAligned && watermarkMillis >= lastOutputWatermark) {
					channelStatuses[channelIndex].isWatermarkAligned = true;
				}
				// 在所有数据通道中找到并输出最小的WaterMark信息
				// now, attempt to find a new min watermark across all aligned channels
				findAndOutputNewMinWatermarkAcrossAlignedChannels();
			}
		}
	}
	// WaterMark传递过程
	private void findAndOutputNewMinWatermarkAcrossAlignedChannels() throws Exception {
		long newMinWatermark = Long.MAX_VALUE;
		boolean hasAlignedChannels = false;
		// 获取最小的WaterMark信息
		// determine new overall watermark by considering only watermark-aligned channels across all channels
		for (InputChannelStatus channelStatus : channelStatuses) {
			if (channelStatus.isWatermarkAligned) {
				hasAlignedChannels = true;
				newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
			}
		}

		// we acknowledge and output the new overall watermark if it really is aggregated
		// from some remaining aligned channel, and is also larger than the last output watermark
		if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
			lastOutputWatermark = newMinWatermark;
			output.emitWatermark(new Watermark(lastOutputWatermark));
		}
	}
    
    // 发送WaterMark之前，如果timeServiceManage不为null时，处理WaterMark，该方法在AbstractStreamOperator对象中
	public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
            // 获取所有timer，并拿出
			timeServiceManager.advanceWatermark(mark);
		}
		output.emitWatermark(mark);
	}
    // 这个触发方式是通过WaterMark来驱动的
	// 处理WaterMark，将满足的定时器获取出来，并触发OnEventTime方法，这个方法在InternalTimerServiceImpl对象中定义
	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer;
		// 获取EventTimeTimersQueue中的所有需要被触发的对象，并逐步触发onEventTime方法
		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			eventTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}

}
```  

### WaterMark在窗口中触发的过程
待补充:（只能在WindowStream触发生成,AllWindowStream需要看看怎么传递的）  
```java
public class WindowedStream<T, K, W extends Window> {
	// WindowStream.apply
	private <R> SingleOutputStreamOperator<R> apply(InternalWindowFunction<Iterable<T>, R, K, W> function, TypeInformation<R> resultType, Function originalFunction) {
		// 生成操作名称，同时将keySelector获取出来
		final String opName = generateOperatorName(windowAssigner, trigger, evictor, originalFunction, null);
		KeySelector<T, K> keySel = input.getKeySelector();

		WindowOperator<K, T, Iterable<T>, R, W> operator;
		// 构建WindowOperator
		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));
			// 构建状态缓存window数据
			ListStateDescriptor<StreamRecord<T>> stateDesc =
					new ListStateDescriptor<>("window-contents", streamRecordSerializer);
			// 实例化WindowOperator
			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					function,
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else { // 通过ListStateDescriptor来存储window的内容
			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
				input.getType().createSerializer(getExecutionEnvironment().getConfig()));
			// 构建WindowOperator
			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					function,
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}
		// 将operator增加到Env的transformations对象中
		return input.transform(opName, resultType, operator);
	}    
}
```  
在窗口中处理每一条数据，并注册定时器  
```java
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

	// 调用processElement方法，处理每一条数据
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final Collection<W> elementWindows = windowAssigner.assignWindows( // 为每个元素通过WindowAssigner分配时间窗口
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyedStateBackend().getCurrentKey();
		// todo: 了解一下MergeWindowAssigner的场景 针对的是SessionWindow
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
            // 轮询每一个窗口，处理元素 
			for (W window: elementWindows) {
				// 判断窗口是否延迟，如果延迟则不做处理
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;
				// 窗口中间数据存储到状态缓存中，todo：查看一下ListState状态缓存结构
				windowState.setCurrentNamespace(window);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = window;
				// 处理时间为每个窗口注册定时器;
				// 事件时间为判断WaterMark是否到达窗口的结束边缘，到达则触发计算
				TriggerResult triggerResult = triggerContext.onElement(element);
				// 处理时间均返回的Continue信息
				if (triggerResult.isFire()) {
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
    // onEventTime触发过程  
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}
		// 触发onEventTime方法
		TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());
        // 再次判断窗口是否能触发 
		if (triggerResult.isFire()) {
            // 获取窗口数据内容  
			ACC contents = windowState.get();
			if (contents != null) {
                //处理窗口内容
				emitWindowContents(triggerContext.window, contents);
			}
		}
		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}
    // 调用用户自定义方法处理窗口内容  
	@SuppressWarnings("unchecked")
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		processContext.window = window;
        // 处理方法
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}

}
```
事件时间触发器，事件时间触发过程**完全由WaterMark来驱动**  
```java
public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
	public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately
			return TriggerResult.FIRE; //如果窗口的结束时间大于watermark时间
		} else {
			ctx.registerEventTimeTimer(window.maxTimestamp()); // 触发操作是基于processElement的处理WaterMark元素并发送WaterMark的过程驱动的，
			// 所以注册事件时间定时器，这个只是注册了定时申明在eventTimeTimersQueue中，没有实际的定时功能。
			return TriggerResult.CONTINUE;
		}
	}    
}
```


## 参考demo  
### demo    
```java
public class EventTimeWindowTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 设置事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// 定义侧边输出Tag
		OutputTag<Tuple2<String, Long>> outputTag = new OutputTag("delayData", Types.TUPLE(Types.STRING, Types.LONG)){};
		// 定义输入源
		DataStream<Tuple2<String, Long>> inputStream = env.addSource(new TimeDataSource());
		// 定义周期性WaterMark提取方法
		DataStream<Tuple2<String, Long>> waterMarkStream = inputStream.assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks());
		// 定义keyBy函数 keyBy函数第一个输出是Value类型，第二个输出是Key类型，需要指定类型，否则key的类型会被推到成Tuple
		KeyedStream<Tuple2<String, Long>, String> keyedStream = waterMarkStream.keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value.f0,  Types.STRING);
		// 定义windowFunction
//		keyedStream.timeWindow(Time.seconds(3)).sideOutputLateData(outputTag).apply(new TimeDataWindowFunction());
		// 这里时间窗口设置的是3s，最大延迟是20s，看看是否可以cover到所有数据
		WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(3)).sideOutputLateData(outputTag);
		SingleOutputStreamOperator<Tuple2<String, Long>> sinkStream = windowedStream.apply(new TimeDataWindowFunction());
		// ProcessWindowFunction可以按照操作状态对数据分流
//		SingleOutputStreamOperator<Tuple2<String, Long>> sinkStream = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
//			@Override
//			public void process(
//					String s,
//					Context context,
//					Iterable<Tuple2<String, Long>> elements,
//					Collector<Tuple2<String, Long>> out) throws Exception {
//				System.out.println("this in " + context.window() + "========" + elements.toString());
//				context.output(outputTag, elements);
//			}
//		});
		// 定义侧边输出
//		windowedStream.sideOutputLateData(outputTag);
		// 定义侧边输出
		DataStream<Tuple2<String, Long>> delayStream = sinkStream.getSideOutput(outputTag);
		delayStream.print();
		// 启动执行
		env.execute(EventTimeWindowTest.class.getSimpleName());
	}
}
```

### 结果输出  
```shell script
this in TimeWindow{start=1608033600000, end=1608033603000}========[(aa,1608033600000)]
this in TimeWindow{start=1608033639000, end=1608033642000}========[(aa,1608033640000), (aa,1608033640000)]
5> (aa,1608033620000)
this in TimeWindow{start=1608033648000, end=1608033651000}========[(aa,1608033650000)]
this in TimeWindow{start=1608033699000, end=1608033702000}========[(aa,1608033700000)]
this in TimeWindow{start=1608033708000, end=1608033711000}========[(aa,1608033710000)]
5> (aa,1608033640000)
this in TimeWindow{start=1608033720000, end=1608033723000}========[(aa,1608033720000)]
```