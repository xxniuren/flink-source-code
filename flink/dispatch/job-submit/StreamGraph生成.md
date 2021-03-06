# 将用户定义的链路算子转换成StreamGraph  
## StreamTransformation  
Transformation表示对DataStream的操作，这些transformation是在OperatorTransformer生成时，产生并存储到env.transformations中的。
这些Transformation不只是表示的物理操作，也有虚拟操作例如SelectTransformation、SplitTransformation、PartitionTransformation等，
Transformation通常保留着父Transformation，所以可以依据父子关系构建StreamGraph DAG图，以WordCount为例，当前的transformations为
OneInputTransformation(FlatMap函数，父节点Transformation是SourceTransformation) -> OneInputTransformation(StreamGroupedReduce函数，
父节点Transformation是PartitionTransform，PartitionTransform的父节点是FlatMap) -> SinkTransformation(SinkFunction函数，父节点是OneInputTransformation) 

```java
class StreamGraphGenerator {
	/**
	 * transform被调用时会通过不同的输入Transform决定转换规则，在父节点存在的transform中，会先调用父节点做
	 * transform，然后把父节点和该节点连接起来，transform的本质是为了生成StreamNode和StreamEdge
	 * 在wordCount里面 transform有flatMapTransform、keyAggrTransform、printTransform
	 * flatMapTransform进行transform时，会生成sourceOperator和flatMapOperator，
	 * keyAggrTransform转换时，会生成keyAggrOperator和PartitionEdge
	 */
	private Collection<Integer> transform(Transformation<?> transform) {
		//判断Transformation是否被处理过
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}
		LOG.debug("Transforming " + transform);
		// 这里设置默认最大并行度
		if (transform.getMaxParallelism() <= 0) {
			// if the max parallelism hasn't been set, then first use the job wide max parallelism
			// from the ExecutionConfig.
			int globalMaxParallelismFromConfig = executionConfig.getMaxParallelism();
			if (globalMaxParallelismFromConfig > 0) {
				transform.setMaxParallelism(globalMaxParallelismFromConfig);
			}
		}
		// call at least once to trigger exceptions about MissingTypeInfo
		transform.getOutputType();
		Collection<Integer> transformedIds;
		// 单一输入的Transformation
		if (transform instanceof OneInputTransformation<?, ?>) {
			transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>) transform);
		// 两输入的Transformation
		} else if (transform instanceof TwoInputTransformation<?, ?, ?>) {
			transformedIds = transformTwoInputTransform((TwoInputTransformation<?, ?, ?>) transform);
		// SourceTransformation
		} else if (transform instanceof SourceTransformation<?>) {
			transformedIds = transformSource((SourceTransformation<?>) transform);
		// SinkTransformation	
		} else if (transform instanceof SinkTransformation<?>) {
			transformedIds = transformSink((SinkTransformation<?>) transform);
		} else if (transform instanceof UnionTransformation<?>) {
			transformedIds = transformUnion((UnionTransformation<?>) transform);
		} else if (transform instanceof SplitTransformation<?>) {
			transformedIds = transformSplit((SplitTransformation<?>) transform);
		} else if (transform instanceof SelectTransformation<?>) {
			transformedIds = transformSelect((SelectTransformation<?>) transform);
		} else if (transform instanceof FeedbackTransformation<?>) {
			transformedIds = transformFeedback((FeedbackTransformation<?>) transform);
		} else if (transform instanceof CoFeedbackTransformation<?>) {
			transformedIds = transformCoFeedback((CoFeedbackTransformation<?>) transform);
		} else if (transform instanceof PartitionTransformation<?>) {
			transformedIds = transformPartition((PartitionTransformation<?>) transform);
		} else if (transform instanceof SideOutputTransformation<?>) {
			transformedIds = transformSideOutput((SideOutputTransformation<?>) transform);
		} else {
			throw new IllegalStateException("Unknown transformation: " + transform);
		}
		// 再次判断是是否已经转换
		if (!alreadyTransformed.containsKey(transform)) {
			alreadyTransformed.put(transform, transformedIds);
		}
		if (transform.getBufferTimeout() >= 0) {
			streamGraph.setBufferTimeout(transform.getId(), transform.getBufferTimeout());
		} else {
			streamGraph.setBufferTimeout(transform.getId(), defaultBufferTimeout);
		}
		// 设置Uid
		if (transform.getUid() != null) {
			streamGraph.setTransformationUID(transform.getId(), transform.getUid());
		}
		if (transform.getUserProvidedNodeHash() != null) {
			streamGraph.setTransformationUserHash(transform.getId(), transform.getUserProvidedNodeHash());
		}
	
		if (!streamGraph.getExecutionConfig().hasAutoGeneratedUIDsEnabled()) {
			if (transform instanceof PhysicalTransformation &&
					transform.getUserProvidedNodeHash() == null &&
					transform.getUid() == null) {
				throw new IllegalStateException("Auto generated UIDs have been disabled " +
					"but no UID or hash has been assigned to operator " + transform.getName());
			}
		}
		// 设置资源情况
		if (transform.getMinResources() != null && transform.getPreferredResources() != null) {
			streamGraph.setResources(transform.getId(), transform.getMinResources(), transform.getPreferredResources());
		}
		streamGraph.setManagedMemoryWeight(transform.getId(), transform.getManagedMemoryWeight());
		return transformedIds;
	}
}
```  
1.转换OneInputTransformation(FlatMap函数)和SourceTransformation(Source节点)
  a.判断OneInputTransformation是否存在父节点，如果有父节点则先转换父节点，使用的是深度优先遍历；  
  b.判断当前节点是否已经转换过，已经转换过的话，跳过此次转换；
  c.将transformation信息包装成StreamNode，并保存到StreamGraph.streamNodes中；
  d.连接souce节点和flatMap节点，构建Edge关系；

```java
class StreamGraphGenerator {
    //构建OneInputTransformation转换
	private <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {
		//获取父节点的输入id，以WordCount为例，当前父节点是Source节点，并将父节点进行转换，这里是深度优先遍历
		Collection<Integer> inputIds = transform(transform.getInput());
		//判断父节点是否已经转换过，已经转换过的话，跳过此次转换
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}
		//获取slotSharingGroup
		String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), inputIds);
		//将transformation信息包装成StreamNode，并保存到StreamGraph.streamNodes中
		streamGraph.addOperator(transform.getId(),
				slotSharingGroup,
				transform.getCoLocationGroupKey(),
				transform.getOperatorFactory(),
				transform.getInputType(),
				transform.getOutputType(),
				transform.getName());
		//
		if (transform.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(transform.getId(), transform.getStateKeySelector(), keySerializer);
		}
		//并行度设置
		int parallelism = transform.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			transform.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(transform.getId(), parallelism);
		streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());
		//连接souce节点和flatMap节点，构建Edge关系
		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId, transform.getId(), 0);
		}
		return Collections.singleton(transform.getId());
	}
    //转换source节点
	private <T> Collection<Integer> transformSource(SourceTransformation<T> source) {
		//slotSharingGroup为default，顶点
		String slotSharingGroup = determineSlotSharingGroup(source.getSlotSharingGroup(), Collections.emptyList());
		streamGraph.addSource(source.getId(),
				slotSharingGroup,
				source.getCoLocationGroupKey(),
				source.getOperatorFactory(),
				null,
				source.getOutputType(),
				"Source: " + source.getName());
		//获取输入格式类型，如果输入格式
		if (source.getOperatorFactory() instanceof InputFormatOperatorFactory) {
			streamGraph.setInputFormat(source.getId(),
					((InputFormatOperatorFactory<T>) source.getOperatorFactory()).getInputFormat());
		}
		int parallelism = source.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			source.getParallelism() : executionConfig.getParallelism();
		//设置streamGraph的并行度信息
		streamGraph.setParallelism(source.getId(), parallelism);
		streamGraph.setMaxParallelism(source.getId(), source.getMaxParallelism());
		return Collections.singleton(source.getId());
	}
}
``` 
2.转换OneInputTransformation(StreamGroupedReduce节点)和操作1类似(StreamGroupedReduce的父节点是PartitionTransform)，PartitionTransformation转换
是在StreamGraph中增加虚拟的分区节点，然后构建flatMap节点和StreamGroupedReduce节点的边关系，分区信息会放在边信息里面；
```java
class StreamGraphGenerator {
    // 虚拟分区节点生成逻辑
	private <T> Collection<Integer> transformPartition(PartitionTransformation<T> partition) {
		Transformation<T> input = partition.getInput();
		List<Integer> resultIds = new ArrayList<>();
		// 先转换其父节点，并拿到父节点的transformedIds
		Collection<Integer> transformedIds = transform(input);
		for (Integer transformedId: transformedIds) {
			//在streamGraph中增加虚拟分区节点
			int virtualId = Transformation.getNewNodeId();
			// 虚拟分区节点信息包括：虚拟分区节点Id、分区信息、shffle模式（当前是HASH shuffle）
			streamGraph.addVirtualPartitionNode(
					transformedId, virtualId, partition.getPartitioner(), partition.getShuffleMode());
			//virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, shuffleMode))
			resultIds.add(virtualId);
		}
		// 返回虚拟分区节点的Ids，去构建Edge关系
		return resultIds;
	}
}
// 构建边关系
class StreamGraph {
    private void addEdgeInternal(Integer upStreamVertexID,
    			Integer downStreamVertexID,
    			int typeNumber,
    			StreamPartitioner<?> partitioner,
    			List<String> outputNames,
    			OutputTag outputTag,
    			ShuffleMode shuffleMode) {
        if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
            //先会走这一步，拿到虚拟分区的相关信息
            // 如果虚拟分区节点包含upStreamVertexID
            int virtualId = upStreamVertexID;
            //获取上游实体父节点
            upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
            //获取分区器
            if (partitioner == null) {
                partitioner = virtualPartitionNodes.get(virtualId).f1;
            }
            shuffleMode = virtualPartitionNodes.get(virtualId).f2;
            //这里是构建flatMap和StreamGroupedReduce节点的边依赖关系
            addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag, shuffleMode);
        } else {
            StreamNode upstreamNode = getStreamNode(upStreamVertexID);
            StreamNode downstreamNode = getStreamNode(downStreamVertexID);
            // 如果没有定义分区规则和父子节点存在相同的并行度，则设置为ForwardPartitioner，否则设置为ReblancePartitioner
            if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
                partitioner = new ForwardPartitioner<Object>();
            } else if (partitioner == null) {
                partitioner = new RebalancePartitioner<Object>();
            }
            if (partitioner instanceof ForwardPartitioner) {
                if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
                    throw new UnsupportedOperationException();
                }
            }
            if (shuffleMode == null) {
                shuffleMode = ShuffleMode.UNDEFINED;
            }
            //构建StreamEdge
            StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner, outputTag, shuffleMode);
            // 设置父StreamNode的出边和子StreamNode的入边
            getStreamNode(edge.getSourceId()).addOutEdge(edge);
            getStreamNode(edge.getTargetId()).addInEdge(edge);
        }
    }
}
```
3.转换SinkTransformation(节点StreamSink)，和上面转换类似
```java
class StreamGraphGenerator {
	private <T> Collection<Integer> transformSink(SinkTransformation<T> sink) {
		//转换sink父节点
		Collection<Integer> inputIds = transform(sink.getInput());
		String slotSharingGroup = determineSlotSharingGroup(sink.getSlotSharingGroup(), inputIds);
		//streamGraph中加入Sink
		streamGraph.addSink(sink.getId(),
				slotSharingGroup,
				sink.getCoLocationGroupKey(),
				sink.getOperatorFactory(),
				sink.getInput().getOutputType(),
				null,
				"Sink: " + sink.getName());
		//获取用户定义的操作工厂，加入输出格式
		StreamOperatorFactory operatorFactory = sink.getOperatorFactory();
		if (operatorFactory instanceof OutputFormatOperatorFactory) {
			streamGraph.setOutputFormat(sink.getId(), ((OutputFormatOperatorFactory) operatorFactory).getOutputFormat());
		}
		//并行度设置
		int parallelism = sink.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ? sink.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(sink.getId(), parallelism);
		streamGraph.setMaxParallelism(sink.getId(), sink.getMaxParallelism());
		//增加边映射关系
		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId,
					sink.getId(),
					0
			);
		}
		if (sink.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = sink.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(sink.getId(), sink.getStateKeySelector(), keySerializer);
		}
		return Collections.emptyList();
	}
} 
```
## 总结
StreamGraph转换完成之后，信息有StreamGraph.sources(存放sources信息)，StreamGraph.streamNodes(存放所有转换节点信息)，StreamGraph.sinks(存放sink节点信息)。
![StreamGraph](../pic/StreamGraph数据结构.png)



    
  
  
 
  
   
