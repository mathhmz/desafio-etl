import hashlib
from dagster import AssetKey
import code
# This class is used to generate a hash code for a given input string.
# The hash code is generated using the built-in hash() function in Python.
# The hash code is returned as an integer value.

class HashCodeChecker:
    def __init__(self, asset:str, context, input) -> None:
        self.asset = asset
        self.context = context
        self.input = input

    def generate_hash(self):
        hash_object = hashlib.sha256(self.input.encode('latin-1'))
        hex_dig = hash_object.hexdigest()
        return hex_dig
    
    def check_last_materialization_hash(self, new_hash):
        materialization_event = self.context.instance.get_latest_materialization_event(asset_key = AssetKey(f"{self.asset}"))
        if materialization_event is not None:
            hash_code = materialization_event.asset_materialization.metadata["hash_code"].value
            if hash_code == new_hash:
                return True
            if hash_code is None:
                return True

        
        return False
    
    def pipeline(self):
        new_hash = self.generate_hash()
        return self.check_last_materialization_hash(new_hash)
    
    def __call__(self):
        return self.pipeline()
            