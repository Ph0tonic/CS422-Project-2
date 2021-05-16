# Report - Project 2 - Advanced DB Systems

Author : Bastien Wermeille
Sciper : 308542
Date : 06.05.2021

## 8 - Performance evaluation
As asked during the exercice session if I needed to run the test for every corpus or if I could only run it for corpus 10 and it was told to me that it was ok to simply use corpus 10. I simply ran my test with a single corpus set `10` and query set `10-2` which gave me the following results:
- `ExactNN` : NaN
- `BaseConstruction` : 2.11
- `BaseConstructionBalanced` : 4.33
- `BaseConstructionBroadcast` : 1.02

Regarding the ExactNN, I tried to run it but it never ended so I stopped it after a few minutes.

Here is the graph of the resulting graph for the query :

![graph](img/graph.png)

Regarding the average distance of each query point from each nearest neighbours, here are the result I got :

TODO