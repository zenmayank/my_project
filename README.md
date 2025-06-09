Hi Thanks for giving me this opportunity to solve this question.


This made me aware that my GitHub repo isn't set up, so I'm starting the setup now.

I haven't used Dataflow earlier, I was learning about it, and it supports Apache Beam pipeline or Python
If I want to run my Python code directly, first I need to containerise it again setup will take time, so keeping time in mind

My approach is DAG will have 3 stages:

1. Fetch the data from api and write it to the temp dir
2. Submit pyspark code in the same directory for transformation, which will save file movement and duplication
3. Write the cleaned data into a bucket and load it to BQ, or directly load the data into BQ
4. Connect BQ from Looker
