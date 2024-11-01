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

#include "datatype/timestamp.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "utils/vdatum/vnumeric.h"
#include "utils/vdatum/vtimestamp.h"
#include "utils/vdatum/vdatum.h"

PG_FUNCTION_INFO_V1(vtimestamp_mi_interval);
PG_FUNCTION_INFO_V1(vtimestamp_pl_interval);
PG_FUNCTION_INFO_V1(vtimestamp_in);
PG_FUNCTION_INFO_V1(vtimestamp_out);
PG_FUNCTION_INFO_V1(vextract_time);
PG_FUNCTION_INFO_V1(vtimestamp_trunc);

vtimestamp* buildvtimestamp(int dim, bool *skip)
{
	return (vtimestamp*)buildvdatum(TIMESTAMPOID, dim, skip);
}
/*
 * We are currently sharing some code between timestamp and timestamptz.
 * The comparison functions are among them. - thomas 2001-09-25
 *
 *		timestamp_relop - is timestamp1 relop timestamp2
 *
 *		collate invalid timestamp at the end
 */
Datum
vtimestamp_timestamp_cmp_internal(vtimestamp *vdt1, Timestamp dt2)
{
	int		i;
	vint4	*result;
	Timestamp dt1;	

	result = buildvdatum(INT4OID,vdt1->dim, vdt1->skipref);
#ifdef HAVE_INT64_TIMESTAMP
	for (i = 0; i < VECTOR_SIZE; i++ )
	{
		if (vdt1->skipref[i])
			continue;
		dt1 = DatumGetTimestamp(VDATUM_DATUM(vdt1, i));
		VDATUM_SET_DATUM(result, i, Int32GetDatum((dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0)));
	}
	return PointerGetDatum(result);
#else
	elog(ERROR, "HAVE_INT64_TIMESTAMP must be enabled in vectorize executor.");
#endif
}


/* timestamp_pl_interval()
 * Add an interval to a timestamp data type.
 * Note that interval has provisions for qualitative year/month and day
 *	units, so try to do the right thing with them.
 * To add a month, increment the month, and use the same day of month.
 * Then, if the next month has fewer days, set the day of month
 *	to the last day of month.
 * To add a day, increment the mday, and use the same time of day.
 * Lastly, add in the "quantitative time".
 */
Datum
vtimestamp_pl_interval(PG_FUNCTION_ARGS)
{
	vtimestamp	*vts = (vtimestamp *)PG_GETARG_POINTER(0);
	Interval	*span = PG_GETARG_INTERVAL_P(1);
	Timestamp	timestamp;
	vtimestamp	*result;
	int i;

	result = buildvtimestamp(vts->dim, vts->skipref);

	for(i = 0; i< vts->dim; i++)
	{
		if (vts->skipref[i])
			continue;
		timestamp = DatumGetTimestamp(VDATUM_DATUM(vts, i));
		if (TIMESTAMP_NOT_FINITE(timestamp))
		{
			VDATUM_SET_DATUM(result, i, TimestampGetDatum(timestamp));
		}
		else
		{
			if (span->month != 0)
			{
				struct pg_tm tt,
							 *tm = &tt;
				fsec_t		fsec;

				if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				tm->tm_mon += span->month;
				if (tm->tm_mon > MONTHS_PER_YEAR)
				{
					tm->tm_year += (tm->tm_mon - 1) / MONTHS_PER_YEAR;
					tm->tm_mon = ((tm->tm_mon - 1) % MONTHS_PER_YEAR) + 1;
				}
				else if (tm->tm_mon < 1)
				{
					tm->tm_year += tm->tm_mon / MONTHS_PER_YEAR - 1;
					tm->tm_mon = tm->tm_mon % MONTHS_PER_YEAR + MONTHS_PER_YEAR;
				}

				/* adjust for end of month boundary problems... */
				if (tm->tm_mday > day_tab[isleap(tm->tm_year)][tm->tm_mon - 1])
					tm->tm_mday = (day_tab[isleap(tm->tm_year)][tm->tm_mon - 1]);

				if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
			}

			if (span->day != 0)
			{
				struct pg_tm tt,
							 *tm = &tt;
				fsec_t		fsec;
				int			julian;

				if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				/* Add days by converting to and from Julian */
				julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday) + span->day;
				j2date(julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

				if (tm2timestamp(tm, fsec, NULL, &timestamp) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));
			}

			timestamp += span->time;

			if (!IS_VALID_TIMESTAMP(timestamp))
				ereport(ERROR,
						(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
						 errmsg("timestamp out of range")));

			VDATUM_SET_DATUM(result, i, TimestampGetDatum(timestamp));
		}
	}
	PG_RETURN_POINTER(result);
}

Datum
vtimestamp_mi_interval(PG_FUNCTION_ARGS)
{
	vtimestamp	*vts = (vtimestamp *)PG_GETARG_POINTER(0);
	Interval	*span = PG_GETARG_INTERVAL_P(1);
	Interval	tspan;

	tspan.month = -span->month;
	tspan.day = -span->day;
	tspan.time = -span->time;

	return DirectFunctionCall2(vtimestamp_pl_interval,
							   PointerGetDatum(vts),
							   PointerGetDatum(&tspan));
}


Datum vtimestamp_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vtimestamp_in not supported");
}
Datum vtimestamp_out(PG_FUNCTION_ARGS)
{
	elog(ERROR, "vtimestamp_out not supported");
}

Datum
vextract_time(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	vdatum *vec_value = (vdatum *) PG_GETARG_POINTER(1);
	int i;
	vnumeric *result;

	result = buildvnumeric(vec_value->dim, vec_value->skipref);

	for (i = 0; i < vec_value->dim; i++)
	{
		if (vec_value->skipref[i])
		{
			result->skipref[i] = true;
			continue;
		}

		if (VDATUM_ISNULL(vec_value, i))
		{
			VDATUM_SET_ISNULL(result, i, true);
		}
		else
			VDATUM_SET_DATUM(result, i,  DirectFunctionCall2(extract_time,
					arg1, VDATUM_DATUM(vec_value, i)));
	}

	PG_RETURN_POINTER(result);
}

Datum
vtimestamp_trunc(PG_FUNCTION_ARGS)
{
	Datum arg1 = PG_GETARG_DATUM(0);
	vdatum *vec_value = (vdatum *) PG_GETARG_POINTER(1);
	int i;
	vtimestamp *result;

	result = buildvtimestamp(vec_value->dim, vec_value->skipref);

	for (i = 0; i < vec_value->dim; i++)
	{
		if (vec_value->skipref[i])
		{
			result->skipref[i] = true;
			continue;
		}

		if (VDATUM_ISNULL(vec_value, i))
		{
			VDATUM_SET_ISNULL(result, i, true);
		}
		else
			VDATUM_SET_DATUM(result, i,  DirectFunctionCall2(timestamp_trunc,
					arg1, VDATUM_DATUM(vec_value, i)));
	}

	PG_RETURN_POINTER(result);

}
