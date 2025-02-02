/*
 * Copyright (c) 2024 Gamma Data, Inc. <jackey@gammadb.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_sequence.h" 
#include "catalog/pg_type.h"
#include "catalog/storage_xlog.h"
#include "catalog/toasting.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "nodes/makefuncs.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "storage/predicate.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "storage/ctable_am.h"
#include "storage/gamma_meta.h"

#define GAMMA_META_CV_TABLE_NAME "gammadb_cv_table_%u"
#define GAMMA_META_CV_INDEX_NAME "gammadb_cv_index_%u"
#define GAMMA_META_CV_SEQ_NAME "gammadb_cv_seq_%u"

#define GAMMA_CV_COMPRESS_NONE			0
#define GAMMA_CV_COMPRESS_PGLZ			1
#define GAMMA_CV_COMPRESS_LZ4			2

int gammadb_delta_table_nblocks = 134217728;
int gammadb_cv_compress_method = GAMMA_CV_COMPRESS_PGLZ;


bool
gamma_meta_cv_table(Relation rel, Datum reloptions)
{
	Oid base_rel_oid = RelationGetRelid(rel);
	Oid cv_nsp = get_namespace_oid(GAMMA_NAMESPACE, false);
	Oid cv_rel_oid = InvalidOid;
	char cv_table_name[NAMEDATALEN];
	char cv_index_name[NAMEDATALEN];
	Oid collationObjectId[2];
	Oid classObjectId[2];
	int16 cvidxoptions[2];
	ObjectAddress baseobject, cvobject, seqobject;
	TupleDesc cv_tupledesc;
	Relation cv_rel;
	IndexInfo *index_info;

	sprintf(cv_table_name, GAMMA_META_CV_TABLE_NAME, base_rel_oid);
	sprintf(cv_index_name, GAMMA_META_CV_INDEX_NAME, base_rel_oid);

	/* 1. create heap table */
	cv_tupledesc = CreateTemplateTupleDesc(Natts_gamma_rowgroup);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_rgid,
											"cvno", OIDOID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_attno,
											"attno", INT4OID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_min,
											"min", TEXTOID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_max,
											"max", TEXTOID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_count,
											"count", INT4OID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_mode,
											"mode", INT4OID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_values,
											"values", TEXTOID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_nulls,
											"nulls", TEXTOID, -1, 0);
	TupleDescInitEntry(cv_tupledesc, (AttrNumber)Anum_gamma_rowgroup_option,
											"option", TEXTOID, -1, 0);

	if (gammadb_cv_compress_method == GAMMA_CV_COMPRESS_PGLZ)
	{
		/* do nothing, it is the default method */
	}
	else if (gammadb_cv_compress_method == GAMMA_CV_COMPRESS_LZ4)
	{
		cv_tupledesc->attrs[Anum_gamma_rowgroup_values - 1].attcompression =
														TOAST_LZ4_COMPRESSION;
		cv_tupledesc->attrs[Anum_gamma_rowgroup_nulls - 1].attcompression =
														TOAST_LZ4_COMPRESSION;
	}
	else
	{
		cv_tupledesc->attrs[Anum_gamma_rowgroup_values - 1].attstorage =
														TYPSTORAGE_EXTERNAL;
		cv_tupledesc->attrs[Anum_gamma_rowgroup_nulls - 1].attstorage =
														TYPSTORAGE_EXTERNAL;
	}

	cv_rel_oid = heap_create_with_catalog(cv_table_name,
			cv_nsp,
			rel->rd_rel->reltablespace,
			InvalidOid,
			InvalidOid,
			InvalidOid,
			rel->rd_rel->relowner,
			HEAP_TABLE_AM_OID, /* GAMMA_NOTE: use heap am */
			cv_tupledesc,
			NIL,
			RELKIND_RELATION, /* GAMMA_NOTE: use relation kind */
			rel->rd_rel->relpersistence,
			rel->rd_rel->relisshared,
			RelationIsMapped(rel),
			ONCOMMIT_NOOP,
			reloptions,
			false,
			true,
			true,
			InvalidOid,
			NULL);

	Assert(cv_rel_oid != InvalidOid);

	CommandCounterIncrement();

	/* 2. create index on the cv table */
	/* ShareLock is not really needed here, but take it anyway */
	cv_rel = table_open(cv_rel_oid, ShareLock);

	index_info = makeNode(IndexInfo);
	index_info->ii_NumIndexAttrs = 2;
	index_info->ii_NumIndexKeyAttrs = 2;
	index_info->ii_IndexAttrNumbers[0] = Anum_gamma_rowgroup_rgid;
	index_info->ii_IndexAttrNumbers[1] = Anum_gamma_rowgroup_attno;
	index_info->ii_Expressions = NIL;
	index_info->ii_ExpressionsState = NIL;
	index_info->ii_Predicate = NIL;
	index_info->ii_PredicateState = NULL;
	index_info->ii_ExclusionOps = NULL;
	index_info->ii_ExclusionProcs = NULL;
	index_info->ii_ExclusionStrats = NULL;
#if PG_VERSION_NUM < 170000
	index_info->ii_OpclassOptions = NULL;
#endif
	index_info->ii_Unique = true;
	index_info->ii_ReadyForInserts = true;
	index_info->ii_Concurrent = false;
	index_info->ii_BrokenHotChain = false;
	index_info->ii_ParallelWorkers = 0;
	index_info->ii_Am = BTREE_AM_OID;
	index_info->ii_AmCache = NULL;
	index_info->ii_Context = CurrentMemoryContext;

	collationObjectId[0] = InvalidOid;
	collationObjectId[1] = InvalidOid;

	classObjectId[0] = OID_BTREE_OPS_OID;
	classObjectId[1] = INT4_BTREE_OPS_OID;

	cvidxoptions[0] = 0;
	cvidxoptions[1] = 0;

	index_create(cv_rel,
			cv_index_name,
			InvalidOid,
			InvalidOid,
			InvalidOid,
			InvalidOid,
			index_info,
			list_make2((void*)"cvno", (void*)"attno"),
			BTREE_AM_OID,
			rel->rd_rel->reltablespace,
			collationObjectId,
			classObjectId,
#if PG_VERSION_NUM >= 170000
			NULL,
#endif
			cvidxoptions,
#if PG_VERSION_NUM >= 170000
			NULL,
#endif
			(Datum)0,
			true,
			0,
			true,
			true,
			NULL);

	table_close(cv_rel, NoLock);

	/* 
	 * Register depencency from the cv table to the base table, so that
	 * the cv table will be deleted if the base table is
	 */
	if (!IsBootstrapProcessingMode()) {
		baseobject.classId = RelationRelationId;
		baseobject.objectId = base_rel_oid;
		baseobject.objectSubId = 0;
		cvobject.classId = RelationRelationId;
		cvobject.objectId = cv_rel_oid;
		cvobject.objectSubId = 0;

		recordDependencyOn(&cvobject, &baseobject, DEPENDENCY_INTERNAL);
	}

	CommandCounterIncrement();

	/* 3.check if the toast relation is created */
	AlterTableCreateToastTable(cv_rel_oid, reloptions, AccessExclusiveLock);

	/* 4. init meta page for columnar table */
	seqobject = gamma_meta_create_sequence(rel);

	/* 
	 * Register depencency from the seq table to the base table, so that
	 * the seq table will be deleted if the base table is
	 */
	if (!IsBootstrapProcessingMode()) {
		baseobject.classId = RelationRelationId;
		baseobject.objectId = base_rel_oid;
		baseobject.objectSubId = 0;

		recordDependencyOn(&seqobject, &baseobject, DEPENDENCY_INTERNAL);
	}

	CommandCounterIncrement();
	return true;
}

void
gamma_meta_truncate_cvtable(Oid cvrelid)
{
	SubTransactionId mySubid = GetCurrentSubTransactionId();
	Relation cvrel = table_open(cvrelid, AccessExclusiveLock);

	if (cvrel->rd_createSubid == mySubid ||
#if PG_VERSION_NUM >= 160000
		cvrel->rd_newRelfilelocatorSubid == mySubid
#else
		cvrel->rd_newRelfilenodeSubid == mySubid
#endif
		)
	{
		heap_truncate_one_rel(cvrel);
	}
	else
	{
		Oid			toast_relid;
		ReindexParams reindex_params = {0};

		CheckTableForSerializableConflictIn(cvrel);
#if PG_VERSION_NUM >= 160000
		RelationSetNewRelfilenumber(cvrel, cvrel->rd_rel->relpersistence);
#else
		RelationSetNewRelfilenode(cvrel, cvrel->rd_rel->relpersistence);
#endif

		toast_relid = cvrel->rd_rel->reltoastrelid;
		if (OidIsValid(toast_relid))
		{
			Relation	toastrel = relation_open(toast_relid,
					AccessExclusiveLock);

#if PG_VERSION_NUM >= 160000
			RelationSetNewRelfilenumber(toastrel,
					toastrel->rd_rel->relpersistence);
#else
			RelationSetNewRelfilenode(toastrel,
					toastrel->rd_rel->relpersistence);
#endif
			table_close(toastrel, NoLock);
		}

		/*
		 * Reconstruct the indexes to match, and we're done.
		 */
#if PG_VERSION_NUM >= 170000
		reindex_relation(NULL, cvrelid, REINDEX_REL_PROCESS_TOAST,
				&reindex_params);
#else
		reindex_relation(cvrelid, REINDEX_REL_PROCESS_TOAST,
				&reindex_params);
#endif
	}

	pgstat_count_truncate(cvrel);
	table_close(cvrel, NoLock);

	return;
}

Oid
gamma_meta_get_cv_table_rel(Relation baserel)
{
	Oid base_rel_oid = RelationGetRelid(baserel);
	return gamma_meta_get_cv_table_oid(base_rel_oid);
}

Oid
gamma_meta_get_cv_table_oid(Oid base_rel_oid)
{
	Oid cv_rel_oid = InvalidOid;
	char cv_table_name[NAMEDATALEN];
	Relation cv_rel;
	RangeVar *rv = makeNode(RangeVar);

	sprintf(cv_table_name, GAMMA_META_CV_TABLE_NAME, base_rel_oid);
	rv->schemaname = GAMMA_NAMESPACE;
	rv->relname = (char *)cv_table_name;

	cv_rel = relation_openrv_extended(rv, RowExclusiveLock, true);
	if (cv_rel == NULL)
		return InvalidOid;

	cv_rel_oid = RelationGetRelid(cv_rel);
	relation_close(cv_rel, RowExclusiveLock);

	pfree(rv);

	return cv_rel_oid;
}
/*************************************************************************/
/*********************** Meta Page Part **********************************/

/*
 * Copy from Postgres
 *
 * The "special area" of a sequence's buffer page looks like this.
 */
#define SEQ_MAGIC	  0x1717

typedef struct sequence_magic
{
	uint32		magic;
} sequence_magic;
/*
 * Copy from Postgres //TODO:
 *
 * Initialize a sequence's relation with the specified tuple as content
 */
static void
fill_seq_with_data(Relation rel, HeapTuple tuple)
{
	Buffer		buf;
	Page		page;
	sequence_magic *sm;
	OffsetNumber offnum;

	/* Initialize first page of relation with special magic number */
#if PG_VERSION_NUM < 160000
	buf = ReadBuffer(rel, P_NEW);
#else
    buf = ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL,
									EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK); 
#endif
	Assert(BufferGetBlockNumber(buf) == 0);

	page = BufferGetPage(buf);

	PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic));
	sm = (sequence_magic *) PageGetSpecialPointer(page);
	sm->magic = SEQ_MAGIC;

	/* Now insert sequence tuple */
#if PG_VERSION_NUM < 160000
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
#endif

	/*
	 * Since VACUUM does not process sequences, we have to force the tuple to
	 * have xmin = FrozenTransactionId now.  Otherwise it would become
	 * invisible to SELECTs after 2G transactions.  It is okay to do this
	 * because if the current transaction aborts, no other xact will ever
	 * examine the sequence tuple anyway.
	 */
	HeapTupleHeaderSetXmin(tuple->t_data, FrozenTransactionId);
	HeapTupleHeaderSetXminFrozen(tuple->t_data);
	HeapTupleHeaderSetCmin(tuple->t_data, FirstCommandId);
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	ItemPointerSet(&tuple->t_data->t_ctid, 0, FirstOffsetNumber);

	/* check the comment above nextval_internal()'s equivalent call. */
	if (RelationNeedsWAL(rel))
		GetTopTransactionId();

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	offnum = PageAddItem(page, (Item) tuple->t_data, tuple->t_len,
						 InvalidOffsetNumber, false, false);
	if (offnum != FirstOffsetNumber)
		elog(ERROR, "failed to add sequence tuple to page");

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_seq_rec	xlrec;
		XLogRecPtr	recptr;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

#if PG_VERSION_NUM < 160000
		xlrec.node = rel->rd_node;
#else
		xlrec.locator = rel->rd_locator;
#endif

		XLogRegisterData((char *) &xlrec, sizeof(xl_seq_rec));
		XLogRegisterData((char *) tuple->t_data, tuple->t_len);

		recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

ObjectAddress
gamma_meta_create_sequence(Relation baserel)
{
	CreateStmt *stmt = makeNode(CreateStmt);
	Oid			seqoid;
	ObjectAddress address;
	Relation	rel;
	HeapTuple	tuple;
	TupleDesc	tupDesc;
	Datum		value[SEQ_COL_LASTCOL];
	bool		null[SEQ_COL_LASTCOL];
	Datum		pgs_values[Natts_pg_sequence];
	bool		pgs_nulls[Natts_pg_sequence];
	int			i;
	char		seq_name[NAMEDATALEN];
	RangeVar	*rv_seq_name = makeNode(RangeVar);

	sprintf(seq_name, GAMMA_META_CV_SEQ_NAME, RelationGetRelid(baserel));

	rv_seq_name->schemaname = GAMMA_NAMESPACE;
	rv_seq_name->relname = seq_name;
	rv_seq_name->relpersistence = RELPERSISTENCE_PERMANENT;

	/*
	 * Create relation (and fill value[] and null[] for the tuple)
	 */
	stmt->tableElts = NIL;
	for (i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++)
	{
		ColumnDef  *coldef = makeNode(ColumnDef);

		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = true;
		coldef->is_from_type = false;
		coldef->collOid = InvalidOid;
		coldef->constraints = NIL;
		coldef->location = -1;

		null[i - 1] = false;

		switch (i)
		{
			case SEQ_COL_LASTVAL:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "last_value";
				value[i - 1] = Int64GetDatumFast((int64)1);
				break;
			case SEQ_COL_LOG:
				coldef->typeName = makeTypeNameFromOid(INT8OID, -1);
				coldef->colname = "log_cnt";
				value[i - 1] = Int64GetDatum((int64) 0);
				break;
			case SEQ_COL_CALLED:
				coldef->typeName = makeTypeNameFromOid(BOOLOID, -1);
				coldef->colname = "is_called";
				value[i - 1] = BoolGetDatum(true);
				break;
		}
		stmt->tableElts = lappend(stmt->tableElts, coldef);
	}

	stmt->relation = rv_seq_name;
	stmt->inhRelations = NIL;
	stmt->constraints = NIL;
	stmt->options = NIL;
	stmt->oncommit = ONCOMMIT_NOOP;
	stmt->tablespacename = NULL;
	stmt->if_not_exists = true;

	address = DefineRelation(stmt, RELKIND_SEQUENCE, InvalidOid, NULL, NULL);
	seqoid = address.objectId;
	Assert(seqoid != InvalidOid);

	rel = table_open(seqoid, AccessExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	/* now initialize the sequence's data */
	tuple = heap_form_tuple(tupDesc, value, null);
	fill_seq_with_data(rel, tuple);
	table_close(rel, NoLock);

	/* fill in pg_sequence */
	rel = table_open(SequenceRelationId, RowExclusiveLock);
	tupDesc = RelationGetDescr(rel);

	memset(pgs_nulls, 0, sizeof(pgs_nulls));

	pgs_values[Anum_pg_sequence_seqrelid - 1] = ObjectIdGetDatum(seqoid);
	pgs_values[Anum_pg_sequence_seqtypid - 1] = ObjectIdGetDatum(OIDOID);
	pgs_values[Anum_pg_sequence_seqstart - 1] = Int64GetDatumFast((int64)1);
	pgs_values[Anum_pg_sequence_seqincrement - 1] = Int64GetDatumFast((int64)1);
	pgs_values[Anum_pg_sequence_seqmax - 1] =
			Int64GetDatumFast((int64)(MaxBlockNumber - GAMMA_DELTA_TABLE_NBLOCKS));
	pgs_values[Anum_pg_sequence_seqmin - 1] = Int64GetDatumFast((int64)1);
	pgs_values[Anum_pg_sequence_seqcache - 1] = Int64GetDatumFast((int64)5);
	pgs_values[Anum_pg_sequence_seqcycle - 1] = BoolGetDatum(false);

	tuple = heap_form_tuple(tupDesc, pgs_values, pgs_nulls);
	CatalogTupleInsert(rel, tuple);

	heap_freetuple(tuple);
	table_close(rel, RowExclusiveLock);

	pfree(rv_seq_name);
	return address;
}

uint32
gamma_meta_next_rgid(Relation rel)
{
	Oid	seq_oid = gamma_meta_rgid_sequence_oid(rel);
	return nextval_internal(seq_oid, false);
}

uint32
gamma_meta_max_rgid(Relation rel)
{
	Oid	seq_oid = gamma_meta_rgid_sequence_oid(rel);
	Datum datum = DirectFunctionCall1(pg_sequence_last_value,
									  ObjectIdGetDatum(seq_oid));
	return DatumGetUInt32(datum);
}

Oid
gamma_meta_rgid_sequence_oid(Relation rel)
{
	char		seq_name[NAMEDATALEN];
	Oid			seq_oid;
	RangeVar	*rv_seq_name = makeNode(RangeVar);

	sprintf(seq_name, GAMMA_META_CV_SEQ_NAME, RelationGetRelid(rel));

	rv_seq_name->schemaname = GAMMA_NAMESPACE;
	rv_seq_name->relname = seq_name;
	rv_seq_name->relpersistence = RELPERSISTENCE_PERMANENT;

	seq_oid = RangeVarGetRelid(rv_seq_name, AccessShareLock, false);

	pfree(rv_seq_name);

	return seq_oid;
}

/*************************************************************************/
/********************* Some operations for Gamma Tables ******************/

void
gamma_meta_insert_rowgroup(Relation rel, RowGroup *rg)
{
	Relation cv_rel;
	TupleDesc tupdesc = rel->rd_att;
	ColumnVector *cv;
	int attno;
	Oid cv_rel_oid = gamma_meta_get_cv_table_rel(rel);
	uint32 rgid = rg->rgid;

	cv_rel = relation_open(cv_rel_oid, RowExclusiveLock);

	if (RGHasDelBitmap(rg))
		gamma_meta_insert_delbitmap(cv_rel, rgid, rg->delbitmap, rg->dim);

	for (attno = 0; attno < tupdesc->natts; attno++)
	{
		cv = gamma_rg_get_cv(rg, attno);
		gamma_meta_insert_cv(cv_rel, rgid, attno + 1, cv);
	}

	relation_close(cv_rel, RowExclusiveLock);

	return;
}

void
gamma_meta_insert_delbitmap(Relation cvrel, uint32 rgid, 
											bool *delbitmap, int32 count)
{
	HeapTuple tuple;
	Datum values[Natts_gamma_rowgroup];
	bool nulls[Natts_gamma_rowgroup];

	text *text_data = cstring_to_text_with_len((char*)delbitmap, count);
	Datum datum_data = PointerGetDatum(text_data);

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[Anum_gamma_rowgroup_rgid - 1] = ObjectIdGetDatum(rgid);
	values[Anum_gamma_rowgroup_attno - 1] =
								Int32GetDatum(GammaDelBitmapAttributeNumber);
	nulls[Anum_gamma_rowgroup_min - 1] = true;
	nulls[Anum_gamma_rowgroup_max - 1] = true;
	values[Anum_gamma_rowgroup_count - 1] = Int32GetDatum(count);
	nulls[Anum_gamma_rowgroup_mode - 1] = true;
	values[Anum_gamma_rowgroup_values - 1] = datum_data;
	nulls[Anum_gamma_rowgroup_nulls - 1] = true;
	nulls[Anum_gamma_rowgroup_option - 1] = true;

	tuple = heap_form_tuple(RelationGetDescr(cvrel), values, nulls);
	CatalogTupleInsert(cvrel, tuple);

	pfree(text_data);
	heap_freetuple(tuple);

	return;

}

void
gamma_meta_insert_cv(Relation cvrel,
					 uint32 rgid, int32 attno, ColumnVector *cv)
{
	HeapTuple tuple;
	Datum values[Natts_gamma_rowgroup];
	bool nulls[Natts_gamma_rowgroup];
	StringInfo data = makeStringInfo();
	text *text_data;
	Datum datum_data;
	text *text_nulls;
	Datum datum_nulls;
	int i;
	bool has_null = false;

	gamma_cv_serialize(cv, data);

	text_data = cstring_to_text_with_len(data->data, data->len);
	datum_data = PointerGetDatum(text_data);

	for (i = 0; i < cv->dim; i++)
	{
		if (cv->isnull[i])
		{
			has_null = true;
			break;
		}
	}

	if (has_null)
	{
		text_nulls = cstring_to_text_with_len((char *)cv->isnull,
				GAMMA_COLUMN_VECTOR_SIZE * sizeof(bool));
		datum_nulls = PointerGetDatum(text_nulls);
	}

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	values[Anum_gamma_rowgroup_rgid - 1] = ObjectIdGetDatum(rgid);
	values[Anum_gamma_rowgroup_attno - 1] = Int32GetDatum(attno);
	nulls[Anum_gamma_rowgroup_min - 1] = true;
	nulls[Anum_gamma_rowgroup_max - 1] = true;
	values[Anum_gamma_rowgroup_count - 1] = Int32GetDatum(cv->dim);;
	nulls[Anum_gamma_rowgroup_mode - 1] = true;
	values[Anum_gamma_rowgroup_values - 1] = datum_data;
	if (has_null)
		values[Anum_gamma_rowgroup_nulls - 1] = datum_nulls;
	else
		nulls[Anum_gamma_rowgroup_nulls - 1] = true;
	nulls[Anum_gamma_rowgroup_option - 1] = true;

	tuple = heap_form_tuple(RelationGetDescr(cvrel), values, nulls);
	CatalogTupleInsert(cvrel, tuple);

	heap_freetuple(tuple);

	pfree(data->data);
	pfree(data);

	return;
}

/*
 * The tid of the gstore table is divided into two parts.
 * One part is in the delta table, which is forward, because the delta table
 * is a heap table, so its tid can be used directly; 
 * The other part is the tid in the cvtable, and each CV is considered is a
 * Page, each Page has at most GAMMA_COLUMN_VECTOR_SIZE tuples, and in this
 * part we assume that tid is in reverse order, for example,  the 0th CV
 * represents the last page of the gstore table, and so on.
 */
ItemPointerData
gamma_meta_cv_convert_tid(uint32 rgid, uint16 rowid)
{
	ItemPointerData tid = { 0 };
	ItemPointerSetBlockNumber(&tid, (MaxBlockNumber - rgid));
	ItemPointerSetOffsetNumber(&tid, rowid);
	return tid;
}

void
gamma_meta_set_tid(HeapTuple tuple, uint32 rgid, uint16 rowid)
{
	ItemPointerSetBlockNumber(&tuple->t_self, (MaxBlockNumber - rgid));
	ItemPointerSetOffsetNumber(&tuple->t_self, rowid);
}

uint32
gamma_meta_tid_get_rgid(ItemPointerData tid)
{
	uint32 blockid = ItemPointerGetBlockNumber(&tid);
	if (blockid < GAMMA_DELTA_TABLE_NBLOCKS)
		return blockid;
	else
		return (MaxBlockNumber - blockid);
}

uint16
gamma_meta_tid_get_rowid(ItemPointerData tid)
{
	return ItemPointerGetOffsetNumber(&tid);
}

uint32
gamma_meta_ptid_get_rgid(ItemPointer tid)
{
	uint32 blockid = ItemPointerGetBlockNumber(tid);
	if (blockid < GAMMA_DELTA_TABLE_NBLOCKS)
		return blockid;
	else
		return (MaxBlockNumber - blockid);
}

uint16
gamma_meta_ptid_get_rowid(ItemPointer tid)
{
	return ItemPointerGetOffsetNumber(tid);
}

bool
gamma_meta_tid_is_columnar(ItemPointer tid)
{
	return (ItemPointerGetBlockNumber(tid) > GAMMA_DELTA_TABLE_NBLOCKS);
}

bool
gamma_meta_is_gamma_table(Oid relid)
{
	bool result = false;
	Relation rel = table_open(relid, AccessShareLock);
	if (rel->rd_tableam == ctable_tableam_routine())
		result = true;

	table_close(rel, AccessShareLock);
	return result;
}
