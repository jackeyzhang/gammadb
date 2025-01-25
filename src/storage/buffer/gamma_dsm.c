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

#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/pg_shmem.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#if PG_VERSION_NUM < 170000
#include "utils/resowner_private.h"
#else
#include "lib/ilist.h"
#include "storage/lwlock.h"
#endif

#include "storage/gamma_dsm.h"
#include "storage/gamma_toc.h"

#define PG_DYNSHMEM_CONTROL_MAGIC       0x9a503d32
#define INVALID_CONTROL_SLOT        ((uint32) -1)
#define GAMMA_BUFFER_SEGMENT_DESC (20170712)

/* Backend-local tracking for on-detach callbacks. */
typedef struct dsm_segment_detach_callback
{
	on_dsm_detach_callback function;
	Datum		arg;
	slist_node	node;
} dsm_segment_detach_callback;

/* Backend-local state for a dynamic shared memory segment. */
struct dsm_segment
{
	dlist_node	node;			/* List link in dsm_segment_list. */
	ResourceOwner resowner;		/* Resource owner. */
	dsm_handle	handle;			/* Segment name. */
	uint32		control_slot;	/* Slot in control segment. */
	void	   *impl_private;	/* Implementation-specific private data. */
	void	   *mapped_address; /* Mapping address, or NULL if unmapped. */
	Size		mapped_size;	/* Size of our mapping. */
	slist_head	on_detach;		/* On-detach callbacks. */
};

/* Shared-memory state for a dynamic shared memory segment. */
typedef struct dsm_control_item
{
	dsm_handle	handle;
	uint32		refcnt;			/* 2+ = active, 1 = moribund, 0 = gone */
	size_t		first_page;
	size_t		npages;
	void	   *impl_private_pm_handle; /* only needed on Windows */
	bool		pinned;
} dsm_control_item;

/* Layout of the dynamic shared memory control segment. */
typedef struct dsm_control_header
{
	uint32		magic;
	uint32		nitems;
	uint32		maxitems;
	dsm_control_item item[FLEXIBLE_ARRAY_MEMBER];
} dsm_control_header;

//static bool dsm_init_done = false;
//static dlist_head dsm_segment_list = DLIST_STATIC_INIT(dsm_segment_list);
static dsm_segment *dsm_seg = NULL;
static gamma_toc *dsm_toc = NULL;

static dsm_handle dsm_control_handle;
static dsm_control_header *dsm_control;
static Size dsm_control_mapped_size = 0;
static void *dsm_control_impl_private = NULL;

static dsm_segment * gamma_buffer_dsm_attach(void);
static dsm_segment * gamma_buffer_dsm_create(Size size, int flags);

#define GAMMA_MB (1024 * 1024)

int gammadb_buffers = 128;

static uint64
dsm_control_bytes_needed(uint32 nitems)
{
	return offsetof(dsm_control_header, item)
		+ sizeof(dsm_control_item) * (uint64) nitems;
}

static bool
dsm_control_segment_sane(dsm_control_header *control, Size mapped_size)
{
	if (mapped_size < offsetof(dsm_control_header, item))
		return false;           /* Mapped size too short to read header. */
	if (control->magic != PG_DYNSHMEM_CONTROL_MAGIC)
		return false;           /* Magic number doesn't match. */
	if (dsm_control_bytes_needed(control->maxitems) > mapped_size)
		return false;           /* Max item count won't fit in map. */
	if (control->nitems > control->maxitems)
		return false;           /* Overfull. */
	return true;
}

static void
gamma_buffer_dsm_main_handle(void)
{
	if (UsedShmemSegAddr == NULL)
	{
		dsm_control_handle = DSM_HANDLE_INVALID;
		return;
	}

	dsm_control_handle = ((PGShmemHeader *)UsedShmemSegAddr)->dsm_control;
	return;
}

static void
gamma_buffer_dsm_main_startup(void)
{
	if (IsUnderPostmaster)
	{
		void	   *control_address = NULL;

		gamma_buffer_dsm_main_handle();

		if (dsm_control_handle == DSM_HANDLE_INVALID)
			return;

		/* Attach control segment. */
		dsm_impl_op(DSM_OP_ATTACH, dsm_control_handle, 0,
					&dsm_control_impl_private, &control_address,
					&dsm_control_mapped_size, ERROR);
		dsm_control = control_address;
		/* If control segment doesn't look sane, something is badly wrong. */
		if (!dsm_control_segment_sane(dsm_control, dsm_control_mapped_size))
		{
			dsm_impl_op(DSM_OP_DETACH, dsm_control_handle, 0,
						&dsm_control_impl_private, &control_address,
						&dsm_control_mapped_size, WARNING);

			dsm_control = NULL;
			//LWLockRelease(DynamicSharedMemoryControlLock); 
			return;
		}
	}

	return;
}

static void
gamma_buffer_dsm_main_detach(void)
{
	if (dsm_control != NULL)
	{
		void *dsm_control_address = dsm_control;
		dsm_impl_op(DSM_OP_DETACH, dsm_control_handle, 0,
				&dsm_control_impl_private, &dsm_control_address,
				&dsm_control_mapped_size, WARNING);
		dsm_control = dsm_control_address;
	}

	return;
}

/* init the dsm for ourself */
void
gamma_buffer_dsm_startup(void)
{
	/* only one backend can create gamma buffer for used */
	uint32 nitems;
	int i;
	bool gb_seg_exists = false;

	if (!IsUnderPostmaster)
		return;

	/* only one backend can create gamma buffer for used */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);

	/* Attach control segment. */
	gamma_buffer_dsm_main_startup();

	/* Search the gamma buffer segment which is in constrol segment. */
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		if (dsm_control->item[i].refcnt > 0
				&& dsm_control->item[i].handle == GAMMA_BUFFER_SEGMENT_DESC)
		{
			gb_seg_exists = true;
			break;
		}
	}

	if (gb_seg_exists)
	{
		gamma_buffer_dsm_attach();
		dsm_toc = gamma_toc_attach(GAMMA_TOC_MAGIC, dsm_seg->mapped_address);
	}
	else
	{
		Size size = ((Size)gammadb_buffers) * GAMMA_MB;
		gamma_buffer_dsm_create(size, 0);
		dsm_toc = gamma_toc_create(GAMMA_TOC_MAGIC, dsm_seg->mapped_address, size);
	}

	/* detach the main dsm segment */
	gamma_buffer_dsm_main_detach();

	LWLockRelease(DynamicSharedMemoryControlLock);
	//on_shm_exist();
}

/*
 * Create a segment descriptor.
 */
static dsm_segment *
gamma_buffer_dsm_segment_desc(void)
{
	dsm_segment *seg;

	//if (CurrentResourceOwner)
	//	ResourceOwnerEnlargeDSMs(CurrentResourceOwner);

	seg = MemoryContextAlloc(TopMemoryContext, sizeof(dsm_segment));
	//dlist_push_head(&Vdsm_segment_list, &seg->node);


	/* seg->handle must be initialized by the caller */
	seg->control_slot = INVALID_CONTROL_SLOT;
	seg->impl_private = NULL;
	seg->mapped_address = NULL;
	seg->mapped_size = 0;

	seg->resowner = NULL;//CurrentResourceOwner;
	//if (CurrentResourceOwner)
	//	ResourceOwnerRememberDSM(CurrentResourceOwner, seg);

	//slist_init(&seg->on_detach);

	return seg;
}


dsm_segment *
gamma_buffer_dsm_create(Size size, int flags)
{
	dsm_segment *seg;
	uint32		i;
	uint32		nitems;

	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);

	/* Create a new segment descriptor. */
	seg = gamma_buffer_dsm_segment_desc();

	/*
	 * We need to create a new memory segment.  Loop until we find an
	 * unused segment identifier.
	 */
	for (;;)
	{
		Assert(seg->mapped_address == NULL && seg->mapped_size == 0);
		/* Use even numbers only */
		seg->handle = GAMMA_BUFFER_SEGMENT_DESC;
		if (dsm_impl_op(DSM_OP_CREATE, seg->handle, size, &seg->impl_private,
					&seg->mapped_address, &seg->mapped_size, ERROR))
			break;
	}

	/* Search the control segment for an unused slot. */
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		if (dsm_control->item[i].refcnt == 0)
		{
			dsm_control->item[i].handle = seg->handle;
			/* refcnt of 1 triggers destruction, so start at 2 */
			dsm_control->item[i].refcnt = 2;
			dsm_control->item[i].impl_private_pm_handle = NULL;
			dsm_control->item[i].pinned = false;
			seg->control_slot = i;

			dsm_seg = seg;
			return seg;
		}
	}

	/* Verify that we can support an additional mapping. */
	if (nitems >= dsm_control->maxitems)
	{
		//LWLockRelease(DynamicSharedMemoryControlLock);
		dsm_impl_op(DSM_OP_DESTROY, seg->handle, 0, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, WARNING);
		Assert (seg->resowner != NULL);
		dlist_delete(&seg->node);
		pfree(seg);

		return NULL;
	}

	/* Enter the handle into a new array slot. */
	dsm_control->item[nitems].handle = seg->handle;
	/* refcnt of 1 triggers destruction, so start at 2 */
	dsm_control->item[nitems].refcnt = 2;
	dsm_control->item[nitems].impl_private_pm_handle = NULL;
	dsm_control->item[nitems].pinned = false;
	seg->control_slot = nitems;
	dsm_control->nitems++;
	//LWLockRelease(DynamicSharedMemoryControlLock);

	/* this is the seg for gamma buffers */
	dsm_seg = seg;

	return seg;
}

static dsm_segment *
gamma_buffer_dsm_attach(void)
{
	dsm_segment *seg;
	//dlist_iter	iter;
	uint32		i;
	uint32		nitems;

	/* Unsafe in postmaster (and pointless in a stand-alone backend). */
	Assert(IsUnderPostmaster);


	/* Create a new segment descriptor. */
	seg = gamma_buffer_dsm_segment_desc();
	seg->handle = GAMMA_BUFFER_SEGMENT_DESC;

	/* Bump reference count for this segment in shared memory. */
	//LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		if (dsm_control->item[i].refcnt <= 1)
			continue;

		/* If the handle doesn't match, it's not the slot we want. */
		if (dsm_control->item[i].handle != seg->handle)
			continue;

		/* Otherwise we've found a match. */
		dsm_control->item[i].refcnt++;
		seg->control_slot = i;
		break;
	}
	//LWLockRelease(DynamicSharedMemoryControlLock);

	/*
	 * If we didn't find the handle we're looking for in the control segment,
	 * it probably means that everyone else who had it mapped, including the
	 * original creator, died before we got to this point. It's up to the
	 * caller to decide what to do about that.
	 */
#if 0
	//TODO:
	if (seg->control_slot == INVALID_CONTROL_SLOT)
	{
		dsm_detach(seg);
		return NULL;
	}
#endif

	/* Here's where we actually try to map the segment. */
	dsm_impl_op(DSM_OP_ATTACH, seg->handle, 0, &seg->impl_private,
			&seg->mapped_address, &seg->mapped_size, ERROR);

	dsm_seg = seg;

	return seg;
}

gamma_toc *
gamma_buffer_dsm_toc(void)
{
	return dsm_toc;
}
