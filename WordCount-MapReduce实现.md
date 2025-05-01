# WordCount-MapReduce实现

## 环境配置

+ 系统环境：Ubuntu 22.04

  tips：Windows环境可以使用` wsl --install `

+ 运行环境：Golang

+ 环境安装

  ```
  # 1. 下载 Go 1.15.15（1.15的最终版本）
  wget https://golang.org/dl/go1.15.15.linux-amd64.tar.gz
  
  # 2. 删除可能存在的旧版本
  sudo rm -rf /usr/local/go
  
  # 3. 将新版本解压到 /usr/local 目录
  sudo tar -C /usr/local -xzf go1.15.15.linux-amd64.tar.gz
  
  # 4. 设置环境变量
  echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
  echo 'export GOPATH=$HOME/gopath' >> ~/.profile
  source ~/.profile
  
  # 5. 验证版本
  go version
  
  # 6. 获取源代码
  git clone https://github.com/Zzhihon/MIT6.824.git
  
  # 7. 依赖安装
  go mod tidy 
  
  ```

  

+ 运行指令

  ```
  编译
  go build -race -buildmode=plugin ../mrapps/wc.go
  
  master节点
  go run -race mrmaster.go pg-*.txt
  
  worker节点
  go run -race mrworker.go wc.so
  
  下面运行了一个master节点和三个worker节点
  ```

  ![image-20250430234333495](C:\Users\Radein\AppData\Roaming\Typora\typora-user-images\image-20250430234333495.png)

+ 测试指令：

  ```
  在/src/main目录下
  执行bash test-mr.sh
  
  测试脚本中会启动10个worker，并给每个worker分配独立id
  ```

  ![屏幕截图 2025-04-30 223554](C:\Users\Radein\Pictures\Screenshots\屏幕截图 2025-04-30 223554.png)

![屏幕截图 2025-04-30 223610](C:\Users\Radein\Pictures\Screenshots\屏幕截图 2025-04-30 223610.png)

## 成果

### mr-x-y

map对输入文件经过哈希后分布的分区

### mr-out-x

reduce对分区文件整理后输出的单词统计结果

![image-20250430235350885](C:\Users\Radein\AppData\Roaming\Typora\typora-user-images\image-20250430235350885.png)

![image-20250430235551682](C:\Users\Radein\AppData\Roaming\Typora\typora-user-images\image-20250430235551682.png)

## Master

### 设计点

总结一下就是 coordinator 在启动时就开启一个后台 goroutine 去不断监控 heartbeatCh 和 reportCh 中的数据并做处理，coordinator 结构体的所有数据都在这个 goroutine 中去修改，从而避免了 data race 的问题，对于 worker 请求任务和汇报任务的 rpc handler，其可以创建 heartbeatMsg 和 reportMsg 并将其写入对应的 channel 然后等待该 msg 中的 ok channel 返回即可。

## 结构体设计

### 核心数据字段

1. **基础配置**
   - [nReduce](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html): Reduce任务的总数
   - `nMap`: Map任务的总数，通常等于输入文件数量
   - `phase`: 当前执行阶段（Map、Reduce或Complete）
2. **任务管理**
   - `mapTasks`/`reduceTasks`: 存储所有Map和Reduce任务的数组
   - `completedMapTasks`/`completedReduceTasks`: 已完成任务的计数器
3. **时间监控**
   - `startTime`/`mapPhaseStartTime`/`reducePhaseStartTime`: 记录各阶段开始时间，用于性能分析和日志记录

### 通信机制（基于Channel的事件驱动模型）

Master结构体通过以下通道实现非阻塞并发控制：

1. **`requestTaskCh`**: 接收workers请求任务的通道
   - 当worker请求新任务时，RPC处理器将请求发送到此通道
2. **`mapCompleteCh`/`reduceCompleteCh`**: 接收任务完成通知的通道
   - 当任务完成时，workers通过RPC发送完成信号到这些通道
3. **`timeoutCheckCh`**: 专用于超时检查的通道
   - 后台goroutine定期发送信号触发超时检测，重置超时任务
4. **[statusCh](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)**: 状态查询通道
   - 允许外部查询MapReduce作业完成状态而不干扰正常任务分配流程
   - 使用通道套通道(`chan chan bool`)实现异步响应机制

![屏幕截图 2025-04-30 224012](C:\Users\Radein\Pictures\Screenshots\屏幕截图 2025-04-30 224012.png)

### 监控协程

1. **单线程状态管理**
   - 所有状态变更都在单一goroutine中执行
   - 无需复杂的互斥锁机制保护共享状态
   - 避免了并发访问带来的竞争条件
2. **基于事件的非阻塞架构**
   - 使用`select`语句实现多通道监听和事件驱动模型
   - 只在有事件发生时才处理，避免了轮询开销
   - 实现了高效的异步处理
3. **关注点分离**
   - RPC层仅负责接收请求并转化为消息
   - 核心逻辑集中在调度循环中处理
   - 提高了代码可读性和可维护性
4. **容错与自动恢复**
   - 自动检测并重置失败或超时的任务
   - 即使Worker崩溃，系统仍能继续运行并完成所有任务

![屏幕截图 2025-04-30 224236](C:\Users\Radein\Pictures\Screenshots\屏幕截图 2025-04-30 224236.png)

## Worker

### 设计点

- atomicWriteFile：需要保证 map 任务和 reduce 任务生成文件时的原子性，从而避免某些异常情况导致文件受损，使得之后的任务无法执行下去的 bug。具体方法就是先生成一个临时文件再利用系统调用 `OS.Rename` 来完成原子性替换，这样即可保证写文件的原子性。
- mergeSort or hashSort: 对于 `doReduceTask` 函数，其输入是一堆本地文件（或者远程文件），输出是一个文件。执行过程是在保证不 OOM 的情况下，不断把 `<key,list(intermediate_value)>` 对喂给用户的 reduce 函数去执行并得到最终的 `<key,value>` 对，然后再写到最后的输出文件中去。在本 lab 中，为了简便我直接使用了一个内存哈希表 (map[string][]string) 来将同一个 key 的 values 放在一起，然后遍历该哈希表来喂给用户的 reduce 函数，实际上这样子是没有做内存限制的。在生产级别的 MapReduce 实现中，该部分一定是一个内存+外存来 mergeSort ，然后逐个喂给 reduce 函数的，这样的鲁棒性才会更高

### Map

- **数据读取**：读取分配给它的输入文件内容
- **键值对生成**：使用用户定义的[mapf](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)函数将输入转换为`KeyValue`对
- **哈希分区**：通过[ihash(key) % nReduce](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)决定每个键值对将被哪个Reduce任务处理
- **物理分区存储**：每个分区创建独立文件，以`mr-mapTaskId-reduceTaskId`命名

![image-20250430232833476](C:\Users\Radein\AppData\Roaming\Typora\typora-user-images\image-20250430232833476.png)

### Reduce

- **值聚合**：将每个唯一key的所有value组合成一个列表
- **规约处理**：调用用户定义的[reducef](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)函数处理每个key的值列表
- **结果输出**：将最终结果写入格式化的输出文件

![image-20250430232910944](C:\Users\Radein\AppData\Roaming\Typora\typora-user-images\image-20250430232910944.png)

### Shuffle

- **跨节点数据传输**：通过文件系统传递数据而非网络传输
- **排序与分组**：确保相同key的所有值被归为一组，为Reduce阶段准备数据

![image-20250430233253279](C:\Users\Radein\AppData\Roaming\Typora\typora-user-images\image-20250430233253279.png)

### 完整流程

1. **任务获取**：Worker不断通过RPC调用[requestTask](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)向Master请求任务
2. **任务处理**：根据任务类型执行Map或Reduce处理
3. **错误处理**：提供重试机制确保任务完成通知能送达Master
4. **任务跟踪**：使用[completedTasks](vscode-file://vscode-app/c:/Users/Radein/AppData/Local/Programs/Microsoft VS Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)映射表避免重复处理已完成任务

