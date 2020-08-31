# jar包执行方式说明

## jar包路径

```
任务1-3: /home/2020st40/project/step1-3.jar
任务4: /home/2020st40/project/step4simple.jar
任务4带邻接表版: /home/2020st40/project/step4complex.jar
任务5: /home/2020st40/project/step5.jar
任务5进阶版：/home/2020st40/project/step5_improved.jar
```

## jar包运行方式

### 任务1-3

```
例: hadoop jar /home/2020st40/project/step1-3.jar /user/2020st40/wuxia_novels /user/2020st40/output
```

程序共有两个参数，第一个参数`/user/2020st40/wuxia_novels`是武侠小说所在目录，第二个参数`/user/2020st40/output`是输出文件目录，程序运行结束后任务1、任务2、任务3的输出分别在`/user/2020st40/output/step1_output`、 `/user/2020st40/output/step2_output ` 、`/user/2020st40/output/step3_output`目录下。

### 任务4

```
例: hadoop jar /home/2020st40/project/step4simple.jar /user/2020st40/output/step3_output /user/2020st40/output/step4_output 30
```

程序共有三个参数，第一个参数`/user/2020st40/output/step3_output`是任务3输出文件所在路径，第二个参数`/user/2020st40/output/step4_output`是输出文件路径，第三个参数`30`是程序迭代次数，程序运行结束后结果文件在`/user/2020st40/output/step4_output/PageRankOutcome`目录下。

### 任务5

```
例: hadoop jar /home/2020st40/project/step5.jar /user/2020st40/output/step3_output /user/2020st40/output/step5_output 10
```

程序共有三个参数，各个参数的意义与任务4相同，不再赘述，程序运行结束后结果文件在`/user/2020st40/output/step5_output/LPAOutcome`目录下。

### 任务5进阶版

首先运行任务4带邻接表版本的jar包：

```
例: hadoop jar /home/2020st40/project/step4complex.jar /user/2020st40/output/step3_output /user/2020st40/output/step4forstep5_output 30
```

各个参数的意义与任务4普通版相同，得到的结果文件作为任务5进阶版的输入文件：

```
例: hadoop jar /home/2020st40/project/step5_improved.jar /user/2020st40/output/step4forstep5_output /user/2020st40/output/step5_improved_output 10
```

各个参数的意义与任务5基础版相同，程序运行结束后得到的结果文件在`/user/2020st40/output/step5_improved_output/LPAOutcome`目录下。