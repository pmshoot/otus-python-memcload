import collections


class Accumulator:
    """Накопитель для формирования пачек данных для последующей выгрузки в memcache.
    Данные выдаются пачкой при достижении заданного количества с обнулением хранилища.

    _store: {
        'dev_type': {
            'key': 'values',
            'key': 'values',
            'key': 'values',
            'key': 'values',
            'key': 'values',
            ... >= buf_Size
        , ...
        }
    }
    """
    def __init__(self, buf_size=5):
        self.buf_size = buf_size
        self._store = collections.defaultdict(dict)

    def put(self, dev_type, key, packed):
        """Буферизация и агрегация данных по ключу dev_type"""
        self._store[dev_type][key] = packed

    def pop_ready(self, drain=False):
        """Изъятие данных по ключу dev_type при достижении порога buf_size"""
        for dev_type, data_map in self._store.items():
            if drain or len(data_map.values()) >= self.buf_size:
                if drain:
                    del self._store[dev_type]
                else:
                    self._store[dev_type] = {}
                if data_map:
                    return dev_type, data_map
                return None, None
        return None, None

    def is_empty(self):
        return not self._store

    def clear(self):
        self._store.clear()
