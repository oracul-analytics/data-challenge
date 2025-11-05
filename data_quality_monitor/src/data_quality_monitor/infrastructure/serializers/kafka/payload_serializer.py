from datetime import datetime
import json
import numpy as np


class PayloadOutputSerializer:
    @staticmethod
    def serialize(obj):
        if isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        if isinstance(obj, (np.integer, int)):
            return int(obj)
        if isinstance(obj, (np.floating, float)):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    @staticmethod
    def to_json(obj) -> str:
        return json.dumps(obj, default=PayloadOutputSerializer.serialize)
