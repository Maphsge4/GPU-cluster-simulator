import csv  # 导入csv模块

import matplotlib.pyplot as plt

filename = 'E:\\python_files\\tiresias\\output\\test_1\\cpu.csv'
with open(filename) as f:
    reader = csv.reader(f)
    header_row = next(reader)  # 返回文件的下一行，在这便是首行，即文件头
    # for index, column_header in enumerate(header_row):  # 对列表调用了 enumerate（）来获取每个元素的索引及其值，方便我们提取需要的数据列
    #     print(index, column_header)

    # 从文件中获取最高温度
    times, cpu0, cpu1 = [], [], []
    for row in reader:  # row是行不是列！
        current_time = int(row[0])
        c0 = int(row[1])
        p_c0 = (100.0 * c0) / 64
        c1 = int(row[2])
        p_c1 = (100.0 * c1) / 64
        times.append(current_time)
        cpu0.append(p_c0)
        cpu1.append(p_c1)

# 根据最高温度绘制图形
plt.style.use('seaborn')
fig, ax = plt.subplots()
ax.plot(times, cpu0, c='red', label='cpu0')
ax.plot(times, cpu1, c='blue', label='cpu1')
# 设置图形的格式
ax.set_title("CPU使用分布情况", fontproperties="SimHei", fontsize=24)
ax.set_xlabel('Time(s)', fontproperties="SimHei", fontsize=16)
fig.autofmt_xdate()
ax.set_ylabel("Percentage(%)", fontproperties="SimHei", fontsize=16)
ax.set_ylim(0, 100)
ax.tick_params(axis='both', which='major', labelsize=16)

plt.legend()

plt.show()
