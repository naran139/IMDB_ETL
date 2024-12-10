import hashlib
import pandas as pd
import uuid

def generate_hash(value):
    if isinstance(value,pd.Series):
        value_str = '|'.join(value.astype(str))
        return hashlib.md5(value_str.encode()).hexdigest()
    else:
        return hashlib.md5(str(value).encode()).hexdigest()
    

def generate_uuid():
    return str(uuid.uuid4())