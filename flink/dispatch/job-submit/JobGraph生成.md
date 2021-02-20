# 将StreamGraph转换成JobGraph过程  
## 数据格式定义  
StreamGraph只是用户定义的逻辑执行依赖链路，在StreamGraph中每一个实体算子(Operator)对应一个StreamNode，在JobGraph中会把满足串联在一起的节点
进行串联形成一个节点，以避免数据在各节点之间进行网络传输。生成过程在Client上 
JobGraph的核心对象是JobVertex、JobEdge、IntermediateDataSet  
**Jobvertex**  
一个JobVertex包含一个或多个StreamNode，JobVertex的输入是JobEdge，输出是IntermediateDataSet，JobVertex的成员变量如下  
```java
class JobVertex {
	private final JobVertexID id;
	/** The alternative IDs of the vertex. */
	private final ArrayList<JobVertexID> idAlternatives = new ArrayList<>();
	//所有的操作Id信息
	private final ArrayList<OperatorID> operatorIDs = new ArrayList<>();
	private final ArrayList<OperatorID> operatorIdsAlternatives = new ArrayList<>();
	//产生的数据中间数据集合
	private final ArrayList<IntermediateDataSet> results = new ArrayList<>();
	//输入边
	private final ArrayList<JobEdge> inputs = new ArrayList<>();
    //todo：确认后补充 
    private String invokableClassName;
      
}
``` 
**JobEdge**  
JobEdge是连接IntermidateDataSet和JobVertex的边，表示JobGraph中一个数据流转换通道，JobEdge的主要成员变量如下  
```java
class JobEdge {
	//边连接的JobVertex
	private final JobVertex target;
	/** The distribution pattern that should be used for this job edge. */
	private final DistributionPattern distributionPattern;
	//中间结果数据集
	private IntermediateDataSet source;
	//数据集Id
	private IntermediateDataSetID sourceId;
	/** Optional name for the data shipping strategy (forward, partition hash, rebalance, ...)*/
	private String shipStrategyName;
	/** Optional name for the pre-processing operation (sort, combining sort, ...),
	 * to be displayed in the JSON plan */
	private String preProcessingOperationName;
	/** Optional description of the caching inside an operator, to be displayed in the JSON plan */
	private String operatorLevelCachingDescription;
}
```  
**IntermidateDataSet**  
中间数据集表示JobVertex的输出，该Jobvertex中包含算子会产生的数据集，IntermidateDataSet的主要成员变量如下    
```java
class IntermidateDataSet {
	private final IntermediateDataSetID id; //id
	private final JobVertex producer; //中间数据集的生产JobVertex
	private final List<JobEdge> consumers = new ArrayList<JobEdge>();//消费边信息，可能存在多个消费边，中间数据集的个数和JobEdge个数一致
	// The type of partition to use at runtime
	private final ResultPartitionType resultType;
}
```
**StreamConfig**  
StreamConfig是JobVertex的配置信息，保存了这个算子(operator)在运行是需要的所有配置信息(包括OperatorFactory信息，输入输出数据类型序列化信息等信息)，
这些信息都是通过key/value的形式存储在Configuration中，所有的StreamConfig会被存储到(Map<Integer, StreamConfig>)vertexConfigs中，方便后续调用；

  
## 从StreamGraph转换成JobGraph  
StreamGraph转换成JobGraph先获取PipelineExecutor，可以从源码中查看现有的PipelineExecutor的实现类。核心函数是setChaining，从顶点开始构建JobGraph。 
**启动StreamGraph到JobGraph之前的构建转换Executor**   
```java
class LocalExecutor {
	private JobGraph getJobGraph(Pipeline pipeline, Configuration configuration) {
		// StreamGraph继承Pipline接口，WordCount示例中Pipeline表示的StreamGraph
		if (pipeline instanceof Plan) {
			Plan plan = (Plan) pipeline;
			final int slotsPerTaskManager = configuration.getInteger(
					TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
			final int numTaskManagers = configuration.getInteger(
					ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

			plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
		}
        // 通过FlinkPipelineTranslationUtil来生成JobGraph，1表示默认并行度
		return FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, 1);
	}
}
// 将StreamGraph转换成JobGraph
class FlinkPipelineTranslationUtil {
	public static JobGraph getJobGraph(
			Pipeline pipeline,
			Configuration optimizerConfiguration,
			int defaultParallelism) {
        //通过反射获取PipelineTranslator
		FlinkPipelineTranslator pipelineTranslator = getPipelineTranslator(pipeline);
		// 将pipeline转换成JobGraph，实现类有PlanTranslator(批数据处理)和StreamGraphTranslator(流处理)
		return pipelineTranslator.translateToJobGraph(pipeline,
				optimizerConfiguration,
				defaultParallelism);
	}
}
```
**通过StreamJobGraph生成JobGraph**   
```java
//通过StreamJobGraphGenerator生成JobGraph
class StreamingJobGraphGenerator {
    //核心代码函数是
	private JobGraph createJobGraph() {
	    //...
		//核心代码，构建JobGraph
		setChaining(hashes, legacyHashes, chainedOperatorHashes);
		setPhysicalEdges();
		setSlotSharingAndCoLocation();
		configureCheckpointing();
		jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
		//添加用户提供的自定义文件信息
		JobGraphGenerator.addUserArtifactEntries(streamGraph.getUserArtifacts(), jobGraph);
		// set the ExecutionConfig last when it has been finalized
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException();
		}
		return jobGraph;
	}
    //从顶点开始创建依赖chain
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		// 获取SourceId，从顶点开始转换 source -> flatMap -> aggr -> sink
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}
	//深度优先搜索 从顶点开始构建依赖链路
	private List<StreamEdge> createChain(
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		// 如果已经转换过，则对该节点不做处理
		if (!builtVertices.contains(startNodeId)) {
			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();
			//获取当前节点
			StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
			//获取当前节点的出边
			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				//判断上下游StreamNode是否可以串联 chain
				if (isChainable(outEdge, streamGraph)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}
			//对可以串联的节点继续搜索创建chain，以当前节点为起点，当前节点的下一个节点为目标节点进行进一步串联
			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
			}
			//对不能串联的节点按照下一个节点为起点和目标节点继续串联，创建chain
			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);
			OperatorID currentOperatorId = new OperatorID(primaryHashBytes);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));
			//设置输入和输出的数据类型
			if (currentNode.getInputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
			}
			if (currentNode.getOutputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
			}
			//构建JobVertex，并生成jobVertex的配置信息
			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)
					: new StreamConfig(new Configuration());
			//设置JobVertex的配置信息
			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);
			//如示例WordCount，此处会按照chain关系构建连接关系 (Source + FlatMap) -> Aggr -> PrintSink
			if (currentNodeId.equals(startNodeId)) {
				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				//构建连接关系
				for (StreamEdge edge : transitiveOutEdges) {
                    // 构建JobEdge和IntermidateSet数据集
					connect(startNodeId, edge);
				}
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));
			} else {
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());
				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}
			// 生成了新的operatorId
			config.setOperatorID(currentOperatorId);
			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}  
}
```
上面是Stream转换为JobGraph的代码，具体流程是从顶点开始深度优先遍历，按照StreamNode之间是否可以chain，将StreamNode分组（把可以chain一起的StreamNode连接在一起），然后将分好组
的StreamNodes生成JobVertex，并且生成JobVertex之间的连接关系（JobEdge和IntermidateDataSet）。JobVertex -> IntermidateDataSet -> JobEdge -> JobVertex  

```java
class StreamingJobGraphGenerator { 
	//判断StreamNode之间是否可以串联起来
	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);
		//SimpleUdfStreamOperatorFactory
		StreamOperatorFactory<?> headOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> outOperator = downStreamVertex.getOperatorFactory();
		// 1. 下游的输入边只有一条，即：直连；
		// 2. 输入和输出operator均不为空；
		// 3. 上下节点slotSharingGroup一致；
		// 4. 下游StreamNode的ChainingStrategy是Always，上游StreamNode的ChainingStrategy是Head或Awalys，默认策略是Always 
        //    ChainingStrategy: Always(总是可以连接)、Never(永远不能连接)、Head(它的操作对象不会被绑在前任上，但是继任者可以绑在这个操作对象上，头节点) 
		// 5. 分区是ForwardPartition；
		// 6. 不是批处理；7. 并行度一致；
		// 8. 可以连接
		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& edge.getShuffleMode() != ShuffleMode.BATCH
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}
    //构建JobVertex的JobEdge和IntermidateDataSet
	private void connect(Integer headOfChain, StreamEdge edge) {
		physicalEdgesInOrder.add(edge);
		Integer downStreamvertexID = edge.getTargetId();
		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);
		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());
		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);
		StreamPartitioner<?> partitioner = edge.getPartitioner();
		ResultPartitionType resultPartitionType;
		//通过Shuffle模式获取结果分区类型
		switch (edge.getShuffleMode()) {
			case PIPELINED:
				resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
				break;
			case BATCH:
				resultPartitionType = ResultPartitionType.BLOCKING;
				break;
			case UNDEFINED:
				resultPartitionType = streamGraph.isBlockingConnectionsBetweenChains() ?
						ResultPartitionType.BLOCKING : ResultPartitionType.PIPELINED_BOUNDED;
				break;
			default:
				throw new UnsupportedOperationException("Data exchange mode " +
					edge.getShuffleMode() + " is not supported yet.");
		}

		checkAndResetBufferTimeout(resultPartitionType, edge);
		// 构建JobEdge和数据集关系
		JobEdge jobEdge;
		if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				resultPartitionType);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					resultPartitionType);
		}
		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());
	}
}
``` 

## 总结
StreamGraph转换为JobGraph主要就是将可以合并的StreamNode进行合并形成JobVertex，然后构造JobEdge和IntermidateDataSet，相当于第一层优化。   





