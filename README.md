Saikyo

A local file column store db for learning


I tested the limit of inserting data and found that when the data exceeds 10000 rows, the flush time of file will continue to grow.

I think that with the expansion of binary balanced tree, the range of nodes to be adjusted for inserting nodes will also expand, resulting in the increase of flush cost.

So I will temporarily stop the development of this exercise.