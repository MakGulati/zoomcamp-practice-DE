add pyspark to conda environment
```bash
conda env config vars set PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```
port for spark jobs

```bash
localhost:4040
```
Find and kill all Java processes with 'spark' in the command name
```bash
pkill -f 'spark'
```