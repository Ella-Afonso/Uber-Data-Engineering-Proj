"""
Pipeline Utilities — PySpark UDFs
Shared utility module for the ride-booking Declarative Pipelines project.

Contains reusable PySpark User-Defined Functions (UDFs) for data quality
and validation, intended for use across Silver and Gold pipeline layers.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

@udf(returnType=BooleanType())
def is_valid_email(email):
    """
    Validate whether the given email address conforms to standard format using regex.
    Returns True if valid, False if malformed or None.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if email is None:
        return False
    return re.match(pattern, email) is not None