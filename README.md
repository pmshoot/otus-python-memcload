# MemcLoad

### Парсинг и заливка в memcache поминутную выгрузку логов трекера установленных приложений.

- memc_load.py    - однопоточная версия
- memc_load_th.py - многопоточная версия (threadings) 
- memc_load_mp.py - многопоточная версия (multiprocessing+threadings) 

Одно-поточный режим:

```shell
python memc_load.py --pattern /full/path/*.tsv.gz --dry

--dry         - проверочный запуск без фактической записи в memcache
```

Много-поточный режим через threads:

```shell
python memc_load_th.py --pattern /full/path/*.tsv.gz -w n --dry

-w, --workers - количество запускаемых тредов

--dry         - проверочный запуск без фактической записи в memcache
остальные параметры по python memc_load_th.py help
```

Много-поточный режим через multiprocessing + threads:

```shell
python memc_load_th.py --pattern /full/path/*.tsv.gz -w n -p n --dry

-p, --processes - количество запускаемых параллельный процессов
-w, --workers   - количество запускаемых тредов в каждом процессе

--dry         - проверочный запуск без фактической записи в memcache
остальные параметры по python memc_load_mp.py help
```

Для корректной работы модуля `appsinstalled_pb2.py` требуется выставить переменную окружения 
`PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`