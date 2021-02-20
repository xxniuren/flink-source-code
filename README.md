## Flink源码学习计划  

### 作业调度与执行过程    
#### 学习规划（所有的学习过程需要在源码中有注释，且在文档中有整理）
a. **学习周期**  
   11月16号 - 12月15号 周期一个月  
b. **学习内容**  
   - Flink源码与解析（https://www.bilibili.com/video/BV1Yk4y1q7nX?p=12）10天  
   - 书本（《Flink内核原理与实现》）；3天  
   - pdf 追图索骥：透过源码看懂Flink核心框架及执行流程；5天  
   - 其他博客，待补充  后两者一周  
   - 官网文档（https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html）  
c. **预达目标**  
   - 深入理解Flink的作业执行流程及任务提交过程；  
   - 深入理解Flink流批处理在Flink内部的实现过程；   
           
### 时间与窗口；  
a. **学习周期**  
   12月15号 - 12月23号，周期为一个月    
b. **学习内容**    
   - 书本（《Flink内核原理与实现》）；  
   - Flink源码与解析（https://www.bilibili.com/video/BV1Yk4y1q7nX?p=12）;  
   - 官网文档（https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html）  
   - 其他博客，待补充；  
c. **预达目标**  
   - 深入理解Flink时间窗口语义及内部实现过程；  
                   
### 状态管理语义；   
a. **学习周期**  
   12月23号 - 12月31号    
b. **学习内容**    
   - 书本（《Flink内核原理与实现》）；          
   - 官网文档（https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html）  
   - 云平台Flink项目，统计Task的各指标状态；  
   - 其他博客，待补充；   
c. **预达目标**    
   - 深入理解Flink状态管理语义；    
   
### 应用容错及exactly-once语义；   
a. **学习周期**    
   12月23号 - 12月31号  
b. **学习内容**  
   - 书本（《Flink内核原理与实现》）；  
   - Flink源码与解析（https://www.bilibili.com/video/BV1Yk4y1q7nX?p=12）;  
   - 官网文档（https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html）  
   - 其他博客，待补充；  
c. **预达目标**  
   - 深入理解Flink如何实现应用容错及exactly-once；    
   
### FlinkSql及执行过程    
a. **学习周期**    
   1月1号 - 过年    
b. **学习内容**        
   - 书本（《Flink内核原理与实现》）；           
   - 官网文档（https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html）    
   - calicate原理及实现过程；  
   - 其他博客，待补充  
c. **预达目标**  
   - 深入理解FlinkSql提交过程，掌握calicate执行过程；   
   
### Flink Connector和Sink  
a. **学习周期**  
   年后 - 3月初  
b. **学习内容**   
   - 官网文档（https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/datastream_api.html）  
   - alchemy项目，支持yaml配置文件  
c. **预达目标**  
   - 熟练掌握各类source和sink；  
   
### Flink和SparkStreaming差异性  
- Flink是纯实时流运算，SparkStreaming将数据流按照时间切割成一个个Rdd，每个rdd分别处理，本质是批处理；  
- Flink重要特定是有状态运算，Flink内部将状态分为KeyedState和OperatorState，checkpoint过程进行存储，而SparkStreaming没有类似功能；  
- Flink支持丰富的窗口语义，包括时间窗口、会话窗口、计数窗口等，方便功能开发；  
- Flink支持AtleastOnce、ExactlyOnce等数据处理语义；  

## Flink批处理和Spark批处理的优劣分析  
   
   
## 进度介绍  
作业调度与执行过程处理中，当前到ExecutionGraph生成过程    
   
