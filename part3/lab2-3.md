## Part 3: Different Parallelism 

Please try different levels of parallelism: 2, 3, 4, 5 

### Design 

To try out different levels of parallelism, the configuration for spark-submit was set by: 

```bash
--conf spark.default.parallelism = 2
```

It can be observed that with higher level of parallelism (-> 5), a convergence is achieved. While when parallelism is lower (2 or 3), no convergence was achieved until the maximum iteration was reached. 

In addition, with a parallelism of 5 rather than 4, the convergence is achieved faster. (0.529500s vs 0.549806s)

Parallelism = 2: 
<img src="pics/paral2.png" width="700">

Parallelism = 3:
<img src="pics/paral3.png" width="700">

Parallelism = 4:
<img src="pics/paral4.png" width="700">

Parallelism = 5:
<img src="pics/paral5.png" width="700">
