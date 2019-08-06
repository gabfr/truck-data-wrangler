from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(StringType())
def get_underscore_prefix(s):
    if '://' in s:
        s = s.split('/')[x-1]
    return "_".join(s.split("_")[:-1])
