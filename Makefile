MODULE_big = gammadb

EXTENSION = gammadb
DATA = gammadb--0.1.sql

#ifeq ($(GAMMA), X)
#	DATA += gammax/gammadb--0.1-x.sql
#endif

PGFILEDESC = "Analytic for PostgreSQL"

TESTS = $(wildcard test/sql/*.sql)
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))

REGRESS_OPTS = --inputdir=test --outputdir=test --temp-config ./regress.conf

#src
OBJS += src/gamma.o

#src/executor
OBJS += src/executor/gamma_vec_tablescan.o src/executor/gamma_devectorize.o \
		src/executor/vector_tuple_slot.o src/executor/vec_exec_scan.o \
		src/executor/gamma_vec_ctablescan.o \
		src/executor/gamma_vec_qual.o \
		src/executor/gamma_vec_agg.o \
		src/executor/gamma_vec_result.o \
		src/executor/gamma_vec_sort.o \
		src/executor/gamma_indexscan.o \
		src/executor/gamma_indexonlyscan.o \
		src/executor/gamma_expr.o \
		src/executor/gamma_vec_exec_grouping.o \
		src/executor/gamma_merge.o \
		src/executor/gamma_copy.o

#src/optimizer
OBJS += src/optimizer/gamma_planner.o \
		src/optimizer/gamma_scan_paths.o \
		src/optimizer/gamma_join_paths.o \
		src/optimizer/gamma_upper_paths.o \
		src/optimizer/gamma_vec_checker.o \
		src/optimizer/gamma_vec_converter.o

#src/utils
OBJS += src/utils/utils.o \
		src/utils/gamma_cache.o \
		src/utils/gamma_fmgr.o \
		src/utils/gamma_distinct.o \
		src/utils/gamma_re.o

#src/utils/nodes
OBJS += src/utils/nodes/gamma_nodes.o

#src/utils/vdatum
OBJS += src/utils/vdatum/vdatum.o src/utils/vdatum/vtimestamp.o \
		src/utils/vdatum/vint.o src/utils/vdatum/vfloat.o \
		src/utils/vdatum/vpseudotypes.o src/utils/vdatum/vvarchar.o \
		src/utils/vdatum/vdate.o src/utils/vdatum/vvarlena.o \
		src/utils/vdatum/vnumeric.o src/utils/vdatum/vlike.o \
		src/utils/vdatum/vtid.o

#src/storage/buffer
OBJS += src/storage/buffer/gamma_dsm.o \
		src/storage/buffer/gamma_toc.o \
		src/storage/buffer/gamma_buffer.o \

#src/storage/gstore
OBJS += src/storage/gstore/gamma_meta.o \
		src/storage/gstore/gamma_cv.o \
		src/storage/gstore/gamma_rg.o

#src/storage/gaccess
OBJS += src/storage/gaccess/ctable_am.o \
		src/storage/gaccess/ctable_dml.o \
		src/storage/gaccess/ctable_vec_am.o \
		src/storage/gaccess/gamma_cvtable_am.o

#src/tcop
OBJS += src/tcop/gamma_utility.o

ifeq ($(GAMMA), X)
SRCX := $(shell find gammax -name "*.c")
OBJS += $(SRCX:.c=.o)
#$(warning $(SRCX))
endif

# print vectorize info when compile
# PG_CFLAGS = -fopt-info-vec

PG_CFLAGS = -Wno-int-in-bool-context -Iinclude

ifeq ($(GAMMA), X)
PG_CFLAGS += -Igammax/include -D_GAMMAX_
endif

ifeq ($(AVX2), TRUE)
PG_CFLAGS += -mavx2 -D_AVX2_
endif

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
override pg_regress_clean_files = test/results/ test/regression.diffs test/regression.out tmp_check/ log/
