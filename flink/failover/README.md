# 异常恢复  
Flink支持异常恢复，通过snapsot来保存task和operator快照，当前从flink **job的状态**
和**checkpoint实现failover&端到端exectly-once**两块来说明Flink异常恢复的强大功能；   
flink相对sparkstreaming最大的优势是实时性和状态恢复功能，所以异常恢复绝对是其很强大的点；  

1. 状态管理； https://juejin.cn/post/6844904053512601607     
2. 状态存储流程及状态恢复； https://www.dazhuanlan.com/2019/10/18/5da8c1dca2ae5/?__cf_chl_jschl_tk__=3a58346d495654015468a59405f59640a52b43f4-1609399937-0-AavkBCH6ES5wI2VH_zaaqklYr6S-yq4SNZ4_o5hXLIgEou6kyv0J9_0bweQfH8N4FUJjhUKnDbMlsDjcXRURn7e38cWAim1AOtwTAvG8azbhRYwSEJQgyiDi6-gErqJgCFIs4Gw6eJTQHLatX1VIq16m3BAfzTcruXz_vljvG8fQxgpYFagMBRb2Eo7ypsRyCHRzf3jdCrgmVrbWbrW2jZlcjW740XNXphqsVOScW34CkjRyaZdLzFpNQE5pcuXa0HWBwxB8wBZw0QIaofR3jmMhVf2RZuziWr_HW9c2u7cMXRpVSCx90TGZ5isaQ1EWGm-eStc1jIpK84jeoW-1dmo  
   - key状态和operator状态使用场景和在flink中如何进行存储  
   - 状态如何恢复；  
