MODULE_big = redislog
OBJS = redislog.o
PG_CONFIG = pg_config
SHLIB_LINK += -lhiredis
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
