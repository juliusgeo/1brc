import math
import mmap
import os
from collections import defaultdict
from dataclasses import dataclass
from math import ceil


@dataclass
class Station:
    min: float
    max: float
    count: int
    total: float




from multiprocessing import Process, Queue, cpu_count
from multiprocessing.pool import Pool

def chunk_file(filename):
    file_size = os.path.getsize(filename)
    chunk_bytes = file_size//cpu_count()
    with open(filename, "rb") as f:
        chunks = []
        prev=0
        while prev < file_size:
            f.seek(min(prev+chunk_bytes, file_size))
            f.readline()
            cur_pos = f.tell()
            chunks.append((prev, cur_pos))
            prev = cur_pos
        print(chunks)
    return chunks


def update_station(station, value):
    if not station:
        return [value, value, 1, value]
    else:
        if value < station[0]:
            station[0] = value
        if value > station[1]:
            station[1] = value
        station[2] += 1
        station[3] += value
        return station

def worker(start, end, filename):
    stations={}
    with open(filename, "r+b") as f:
        closest_start = (start//mmap.ALLOCATIONGRANULARITY)*mmap.ALLOCATIONGRANULARITY
        remainder = start-closest_start
        chunk = mmap.mmap(fileno=f.fileno(),length=end-closest_start, offset=closest_start, access=mmap.ACCESS_READ)
        chunk.madvise(mmap.MADV_SEQUENTIAL)
        chunk.seek(remainder)
        for row in iter(chunk.readline, b''):
            name, value = row.split(b';')
            name,value = name, float(value)
            stations[name] = update_station(stations.get(name), value)
    return stations

def merge_stations(station, value):
    if value[0]<station[0]:
        station[0]=value[0]
    if value[1]>station[1]:
        station[1]=value[1]
    station[2]+=value[2]
    station[3]+=value[3]
    return station
def main(filename):
    chunks=chunk_file(filename)
    with Pool() as pool:
        workers=[pool.apply_async(worker,args=(*chunk,filename)) for chunk in chunks]
        stations={}
        for d in [w.get() for w in workers]:
            for key,value in d.items():
                if key in stations:
                    stations[key]=merge_stations(stations[key],value)
                else:
                    stations[key]=value
        def round(x):
            return (math.ceil(x * 10) / 10)
        for station in stations:
            minimum,maximum,count,total=stations[station]
            stations[station]=f"{minimum:.1f}/{round(total/count):.1f}/{maximum:.1f}"
        return "{"+', '.join([key.decode("utf-8")+"="+stations[key] for key in sorted(stations.keys())])+"}"


if __name__ == '__main__':
    filename="measurements.txt"
    print(main(filename))