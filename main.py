import math
import mmap
import os
from multiprocessing import cpu_count

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
    return chunks

def worker(start, end, filename):
    stations={}
    f = os.open(filename, flags=os.O_RDONLY)
    closest_start = (start//mmap.ALLOCATIONGRANULARITY)*mmap.ALLOCATIONGRANULARITY
    remainder = start-closest_start
    chunk = mmap.mmap(fileno=f,length=end-closest_start, offset=closest_start, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ)
    chunk.madvise(mmap.MADV_SEQUENTIAL)
    chunk.madvise(mmap.MADV_DONTNEED, 0, remainder)
    chunk.seek(remainder)
    for row in iter(chunk.readline, b''):
        name, value = row.split(b';')
        value = float(value)
        if name not in stations:
            stations[name] = [value,value,0,0]
        station=stations[name]
        if value<station[0]:
            station[0]=value
        if value>station[1]:
            station[1]=value
        station[2]+=1
        station[3]+=value

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
    import concurrent.futures as cf
    with cf.ProcessPoolExecutor() as pool:
        workers=[pool.submit(worker, *chunk, filename) for chunk in chunks]
        stations={}
        for d in cf.as_completed(workers):
            for key,value in d.result().items():
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