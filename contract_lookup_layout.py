"""Cluster award rows on disk with bounded sorts and exact streaming validation.

Each original row window is sorted by contract ID and original row position.
Window order and equal-ID order are retained, preserving the first record for
legacy `WHERE contract_id = ? LIMIT 1` lookups, including duplicate identifiers.
No records, columns, values, or serving-reader limits are changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import tempfile
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def literal(value):
    return "'"+str(value).replace("'", "''")+"'"


def connection(memory_mb, spill, *, preserve_order=False):
    if not 128 <= memory_mb <= 512:
        raise ValueError('Lookup layout memory budget must be 128–512 MB')
    c=duckdb.connect(config={'memory_limit':f'{memory_mb}MB','threads':1})
    c.execute(f'SET temp_directory={literal(spill)}')
    c.execute("SET max_temp_directory_size='4GB'")
    c.execute(f'SET preserve_insertion_order={str(preserve_order).lower()}')
    return c


def window_sql(source, start, stop):
    return (f'SELECT * EXCLUDE(file_row_number) FROM read_parquet({literal(source)}, file_row_number=true) '
            f'WHERE file_row_number >= {start} AND file_row_number < {stop} '
            'ORDER BY contract_id ASC NULLS LAST, file_row_number ASC')


def arrow_reader(cursor, rows=4096):
    method=getattr(cursor,'to_arrow_reader',None) or cursor.fetch_record_batch
    return method(rows)


def equal_batches(left, right):
    if not left.schema.equals(right.schema,check_metadata=False):
        return False
    for a,b in zip(left.columns,right.columns):
        # Compare floating-point bit patterns, including signed zero and NaNs.
        if pa.types.is_floating(a.type):
            unsigned={32:pa.uint32(),64:pa.uint64()}[a.type.bit_width]
            a,b=a.view(unsigned),b.view(unsigned)
        if not a.equals(b):return False
    return True


def verify_layout(source, candidate, *, memory_mb=256, window_rows=2_000_000, spill, progress=None):
    """Compare every field of every row, retaining equal-ID selection order."""
    c=connection(memory_mb,spill,preserve_order=True)
    left=c.cursor();right=c.cursor()
    try:
        before=c.execute(f'DESCRIBE SELECT * FROM read_parquet({literal(source)})').fetchall()
        after=c.execute(f'DESCRIBE SELECT * FROM read_parquet({literal(candidate)})').fetchall()
        if before!=after:raise ValueError('Candidate schema differs')
        rows=int(c.execute(f'SELECT num_rows FROM parquet_file_metadata({literal(source)})').fetchone()[0])
        actual_rows=int(c.execute(f'SELECT num_rows FROM parquet_file_metadata({literal(candidate)})').fetchone()[0])
        if rows!=actual_rows:raise ValueError('Candidate row count differs')
        actual=arrow_reader(right.sql(f'SELECT * FROM read_parquet({literal(candidate)})'))
        pending=None;pending_offset=0;compared=0
        next_notice=250000
        for start in range(0,rows,window_rows):
            expected=arrow_reader(left.sql(window_sql(source,start,min(start+window_rows,rows))))
            for batch in expected:
                offset=0
                while offset<batch.num_rows:
                    if pending is None:
                        pending=next(actual);pending_offset=0
                    count=min(batch.num_rows-offset,pending.num_rows-pending_offset)
                    if not equal_batches(batch.slice(offset,count),pending.slice(pending_offset,count)):
                        raise ValueError(f'Candidate row values/order differ at offset {compared}')
                    offset+=count;pending_offset+=count;compared+=count
                    if pending_offset==pending.num_rows:pending=None
                    if progress and compared>=next_notice:
                        progress({'phase':'parity','rows_compared':compared,'rows':rows})
                        next_notice=compared+250000
        if pending is not None or next(actual,None) is not None or compared!=rows:
            raise ValueError('Candidate contains extra or missing rows')
        return {'rows_compared':compared,'columns_compared':len(before),'all_values_equal':True,
                'duplicate_id_selection_order_preserved':True}
    finally:
        left.close();right.close();c.close()


def sort_lookup_file(source, destination, *, memory_mb=256, window_rows=2_000_000, temp_root=None, progress=None):
    source,destination=Path(source).resolve(),Path(destination).resolve()
    if source==destination or destination.exists():
        raise ValueError('Candidate must be a new path, distinct from its source')
    if window_rows<1:raise ValueError('window_rows must be positive')
    destination.parent.mkdir(parents=True,exist_ok=True)
    temp_root=Path(temp_root or destination.parent)
    temp_root.mkdir(parents=True,exist_ok=True)
    began=time.monotonic()
    with tempfile.TemporaryDirectory(prefix='lookup-sort-',dir=temp_root) as work:
        work=Path(work);candidate=work/'candidate.parquet'
        c=connection(memory_mb,work/'spill-build')
        writer=None
        try:
            columns=[r[0] for r in c.execute(f'DESCRIBE SELECT * FROM read_parquet({literal(source)})').fetchall()]
            if 'contract_id' not in columns or 'file_row_number' in columns:
                raise ValueError('Source lacks contract_id or collides with the ordinal column')
            rows=int(c.execute(f'SELECT num_rows FROM parquet_file_metadata({literal(source)})').fetchone()[0])
            if rows==0:
                c.execute(f'COPY (SELECT * FROM read_parquet({literal(source)})) TO {literal(candidate)} (FORMAT PARQUET,COMPRESSION ZSTD)')
            for start in range(0,rows,window_rows):
                window=work/f'window-{start}.parquet'
                if progress:progress({'phase':'sort','start':start,'rows':min(window_rows,rows-start)})
                c.execute(f'COPY ({window_sql(source,start,min(start+window_rows,rows))}) TO {literal(window)} '
                          '(FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 100000)')
                if progress:progress({'phase':'sort_complete','start':start,'bytes':window.stat().st_size})
                if rows<=window_rows:
                    window.replace(candidate)
                else:
                    parquet=pq.ParquetFile(window)
                    if writer is None:writer=pq.ParquetWriter(candidate,parquet.schema_arrow,compression='zstd')
                    for batch in parquet.iter_batches(batch_size=100000):writer.write_batch(batch,row_group_size=100000)
                    parquet.close();window.unlink()
        finally:
            if writer is not None:writer.close()
            c.close()
        proof=verify_layout(source,candidate,memory_mb=memory_mb,window_rows=window_rows,spill=work/'spill-verify',progress=progress)
        candidate.replace(destination)
    result={**proof,'source':str(source),'candidate':str(destination),'memory_limit_mb':memory_mb,
            'window_rows':window_rows,'candidate_bytes':destination.stat().st_size,'seconds':round(time.monotonic()-began,3)}
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('source');p.add_argument('destination');p.add_argument('--memory-mb',type=int,default=256)
    p.add_argument('--window-rows',type=int,default=2_000_000);p.add_argument('--temp-root');p.add_argument('--report')
    args=p.parse_args()
    result=sort_lookup_file(args.source,args.destination,memory_mb=args.memory_mb,window_rows=args.window_rows,temp_root=args.temp_root,
                            progress=lambda row:print(json.dumps(row),flush=True))
    if args.report:Path(args.report).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
