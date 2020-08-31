# Jin-Yong-novels-characters-relationship-mining
Using Java programming language and data mining methods to analysis Jin Yong novels' characters' relationship through Hadoop MapReduce framework

"金庸的江湖"——金庸武侠小说中的人物关系挖掘

利用 Hadoop MapReduce 并行处理程序设计框架和 Java 编程语言，完成针对金庸17本武侠小说的人物关系数据分析。作为一个综合性的数据挖掘任务，处理包括全流程的数据预处理、数据分析、数据后处理等。
该工程实现了以下内容：
1. 在 Hadoop 中使用第三方的 Jar 包来辅助分析，使用 Ansj_seg 工具对原始文本进行分词;
2. 常用的 MapReduce 算法设计:
a) 单词同现算法;
b) 数据整理与归一化算法; 
c) 数据排序;
3. 带有迭代特性的数据挖掘 MapReduce 算法设计:
a) PageRank 算法;
b) 标签传播(Label Propagation)算法
4. 数据可视化实现
