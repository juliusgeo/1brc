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

from brc import update_dict


def merge_stations(station, value):
    if value[0]<station[0]:
        station[0]=value[0]
    if value[1]>station[1]:
        station[1]=value[1]
    station[2]+=value[2]
    station[3]+=value[3]
    return station

def merge_and_format(worker_results):
    stations = {}
    for d in worker_results:
        for key,value in d.result().items():
            if key in stations:
                stations[key]=merge_stations(stations[key],value)
            else:
                stations[key]=value

    def round(x):
        return (math.ceil(x*10)/10)

    for station in stations:
        minimum,maximum,count,total=stations[station]
        stations[station]=f"{minimum:.1f}/{round(total/count):.1f}/{maximum:.1f}"
    return stations
def main(filename):
    chunks=chunk_file(filename)
    import concurrent.futures as cf
    with cf.ProcessPoolExecutor() as pool:
        workers=[pool.submit(update_dict, *chunk, filename) for chunk in chunks]
        stations=merge_and_format(cf.as_completed(workers))
        return "{"+', '.join([key+"="+stations[key] for key in sorted(stations.keys())])+"}"


if __name__ == '__main__':
    filename="measurements.txt"
    print(main(filename))