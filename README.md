# MPLab4 K-Means聚类算法

黄铭昊 林哲浩 张侃

## 基本概述

### 小组分工

* 黄铭昊：负责框架构建、测试；
* 林哲昊：负责选做；
* 张侃：负责基本功能。

### 项目管理

采用Github进行项目管理。

Github仓库：https://github.com/KitasanMPLab/MPLab4

## 框架

### VectorDoubleWritable

为了存储向量，需要定义一个数据类型`VectorDoubleWritable`：

```java
public class VectorDoubleWritable extends Vector<DoubleWritable> implements Writable, Comparable<VectorDoubleWritable>
```

* 支持向量加法、乘法、除法、求距离操作；

* 同时还支持用`Text`或`String`类型初始化操作，格式形如`xxx,xxx,xxx,...,xxx`；

* 还支持`Writable`和`Comparable`的接口操作，以及转化成字符串的操作。

### PairVectorDoubleInt

为了方便后续Map和Reduce的设计，需要设计一个键值对类型`<VectorDoubleInt, IntWritable>`：

```java
public class PairVectorDoubleInt extends PairWritable<VectorDoubleWritable, IntWritable>
```

其中`PairWritable`实现方式和实验2的`PairWritableComparable`类似。

### CenterFileOperation

`CenterFileOperation`用于支持聚类中心文件的读写操作：

* 将聚类中心存储到文件中；
* 从文件中读取聚类中心；
* 从文件夹中读取聚类中心。

### Job迭代

输入为`<center-in>`和`<point-in>`，输出为`<k-means-out>`和`<cluster-out>`。

设立一个临时**文件**，用于存储上一次聚类中心结果，存放在`<k-means-out>/temp`中。

每次聚类中心的结果存放在`<k-means-out>/final`**文件夹**中。

具体的核心代码如下：

```java
//将initial_centers存放到<k-means-out>/temp中
initKMeansOut(conf, centerIn, kMeansOut, tempKMeansOut);
do {
    //清理上一次聚类中心结果
    clearKMeansOut(conf, finalKMeansOut);
    //运行kMeansJob
    Job kMeansJob = getKMeansJob(conf, tempKMeansOut, pointIn, finalKMeansOut);
    boolean flag = kMeansJob.waitForCompletion(true);
    if (!flag) System.exit(1);
    //比较这次聚类中心和上一次聚类中心结果，并将这次结果覆盖到<k-means-out>/temp中，如果收敛则结束循环
} while(!check(conf, tempKMeansOut, finalKMeansOut));
//运行clusterJob
Job clusterJob = getClusterJob(conf, tempKMeansOut, pointIn, clusterOut);
System.exit(clusterJob.waitForCompletion(true) ? 0 : 1);
```

## 基本功能

### Map



### Combine



### Reduce



## 选做

### Map



### Reduce



## 实验结果

在实验平台运行命令：

` hadoop jar MPLab4/MPLab4.jar /data/2022s/kmeans/initial_centers /data/2022s/kmeans/dataset.data MPLab4/kmeans MPLab4/cluster`

### 基本功能



### 选做



### WebUI执行报告

#### 基本功能



#### 选做