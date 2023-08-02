# Simulator

## 代码整体架构
```
├─ simulator  -- 代码根目录
│  ├─ client
│     ├── jobs.py
│     ├── model.py
│  ├─ input
│     ├── trace-job.csv
│     ├── nxgx.csv
│     ├── yarn-distribution.csv
│  ├─ output
│     ├── debug
│         ├── debug-xxx.txt
│     ├── cluster.csv
│     ├── job.csv
│     ├── cpu.csv
│     ├── gpu.csv
│     ├── network.csv
│  ├─ placement
│     ├── base.py
│     ├── count.py
│     ├── random.py
│     ├── yarn.py
│  ├─ scheduler
│     ├── dlas.py
│     ├── fifo.py
│     ├── fjf.py
│     ├── shortest.py
│  ├─ server
│     ├── cluster.py
│     ├── node.py
│     ├── switch.py
│  ├─ utils
│     ├── drawCPU.py
│     ├── drawGPU.py
│     ├── flags.py
│     ├── log.py
│     ├── util.py
│  ├─ batch_run.sh
│  ├─ run_sim.py
```
各目录和文件作用如下：
1. client目录
存放与深度神经网络模型有关的参数，负责解析用户输入的trace文件。
2. input目录
存放用户输入的trace文件和集群配置信息。
3. output目录
存放输出的日志文件，主要包括：（1）调试信息；（2）集群整体使用情况随时间的变化；（3）CPU/GPU/网络使用情况随时间的变化；（4）AI任务运行完成记录文件。
4. placement目录
目录中有不同资源分配策略的代码实现。
5. scheduler目录
目录中有不同调度器及相关函数的代码实现。
6. server目录
存放集群、机架、节点类。
7. utils目录
存放模拟器的功能性函数，如命令行参数解析、日志文件生成与可视化等。
8. run_sim.py文件
主函数所在文件。
9. batch_run.sh文件
批量运行bash命令，执行此文件运行模拟器。