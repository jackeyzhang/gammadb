/*-------------------------------------------------------------------------
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/nodes/gamma_nodes.h"

/********************************* copyfuncs *********************************/
/********************************* copyfuncs *********************************/
/********************************* copyfuncs *********************************/

/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObjectImpl(from->fldname))

/* Copy a field that is a pointer to a Bitmapset */
#define COPY_BITMAPSET_FIELD(fldname) \
	(newnode->fldname = bms_copy(from->fldname))

/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) \
	(newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char *) NULL)

/* Copy a field that is an inline array */
#define COPY_ARRAY_FIELD(fldname) \
	memcpy(newnode->fldname, from->fldname, sizeof(newnode->fldname))

/* Copy a field that is a pointer to a simple palloc'd object of size sz */
#define COPY_POINTER_FIELD(fldname, sz) \
	do { \
		Size	_size = (sz); \
		if (_size > 0) \
		{ \
			newnode->fldname = palloc(_size); \
			memcpy(newnode->fldname, from->fldname, _size); \
		} \
	} while (0)

/* Copy a parse location field (for Copy, this is same as scalar case) */
#define COPY_LOCATION_FIELD(fldname) \
	(newnode->fldname = from->fldname)



/********************************** outfuncs *********************************/
/********************************** outfuncs *********************************/
/********************************** outfuncs *********************************/
/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
	appendStringInfoString(str, nodelabel)

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write an unsigned integer field (anything written with UINT64_FORMAT) */
#define WRITE_UINT64_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " " UINT64_FORMAT, \
					 node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %ld", node->fldname)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", \
					 (int) node->fldname)

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %s", \
					 booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfoString(str, " :" CppAsString(fldname) " "), \
	 outBitmapset(str, node->fldname))

#define WRITE_ATTRNUMBER_ARRAY(fldname, len) \
	do { \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (int i = 0; i < len; i++) \
			appendStringInfo(str, " %d", node->fldname[i]); \
	} while(0)

#define WRITE_OID_ARRAY(fldname, len) \
	do { \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (int i = 0; i < len; i++) \
			appendStringInfo(str, " %u", node->fldname[i]); \
	} while(0)

/*
 * This macro supports the case that the field is NULL.  For the other array
 * macros, that is currently not needed.
 */
#define WRITE_INDEX_ARRAY(fldname, len) \
	do { \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		if (node->fldname) \
			for (int i = 0; i < len; i++) \
				appendStringInfo(str, " %u", node->fldname[i]); \
		else \
			appendStringInfoString(str, "<>"); \
	} while(0)

#define WRITE_INT_ARRAY(fldname, len) \
	do { \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (int i = 0; i < len; i++) \
			appendStringInfo(str, " %d", node->fldname[i]); \
	} while(0)

#define WRITE_BOOL_ARRAY(fldname, len) \
	do { \
		appendStringInfoString(str, " :" CppAsString(fldname) " "); \
		for (int i = 0; i < len; i++) \
			appendStringInfo(str, " %s", booltostr(node->fldname[i])); \
	} while(0)


#define booltostr(x)  ((x) ? "true" : "false")



/**********************************   utils  *********************************/
/**********************************   utils  *********************************/
/**********************************   utils  *********************************/
static void
copyGammaNodeWrapper(struct ExtensibleNode *newnode,
						 const struct ExtensibleNode *oldnode)
{
	ereport(ERROR, (errmsg("not implemented")));
}

static bool
equalGammaNodeWrapper(const struct ExtensibleNode *a,
						  const struct ExtensibleNode *b)
{
	ereport(ERROR, (errmsg("not implemented")));
}

static void
readGammaNodeWrapper(struct ExtensibleNode *node)
{
	ereport(ERROR, (errmsg("not implemented")));
}

static void
outGammaNodeWrapper(StringInfo info, const struct ExtensibleNode *node)
{
	ereport(ERROR, (errmsg("not implemented")));
}

/* *INDENT-OFF* */
#define DEFINE_NODE_METHODS(type) \
	{ \
		#type, \
		sizeof(type), \
		copyGammaNodeWrapper, \
		equalGammaNodeWrapper, \
		outGammaNodeWrapper, \
		readGammaNodeWrapper, \
	}

const ExtensibleNodeMethods GammaNodeMethods[] =
{
	DEFINE_NODE_METHODS(gamma_plan_type),
};

const char* GammaTagNames[] = 
{
	"gamma_plan_type",
};

void
gamma_register_nodes(void)
{
	int i;
	for (i = 0; i < lengthof(GammaNodeMethods); i++)
	{
		RegisterExtensibleNodeMethods(&GammaNodeMethods[i]);
	}
}
