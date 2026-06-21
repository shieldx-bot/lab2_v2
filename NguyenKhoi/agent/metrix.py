import time

import psutil

while True:
    # snapshot đầu
    disk1 = psutil.disk_io_counters()
    net1 = psutil.net_io_counters()

    time.sleep(1)

    # snapshot sau
    disk2 = psutil.disk_io_counters()
    net2 = psutil.net_io_counters()

    cpu_usage = psutil.cpu_percent()
    memory_usage = psutil.virtual_memory().percent

    disk_io = (
        (
            (disk2.read_bytes - disk1.read_bytes)
            + (disk2.write_bytes - disk1.write_bytes)
        )
        / 1024
        / 1024
    )  # MB/s

    network_io = (
        ((net2.bytes_recv - net1.bytes_recv) + (net2.bytes_sent - net1.bytes_sent))
        / 1024
        / 1024
    )  # MB/s

    metric_data = {
        "CPU_Usage": round(cpu_usage, 2),
        "Memory_Usage": round(memory_usage, 2),
        "Disk_IO": round(disk_io, 2),
        "Network_IO": round(network_io, 2),
    }

    print(metric_data)
