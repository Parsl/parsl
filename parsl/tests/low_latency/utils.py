import subprocess


def ping_time(ip, n=5):
    """
    Returns the average ping time in microseconds.

    Note: This function is inherently platform specific.
    It currently works on Midway.
    """
    cmd = "ping {} -c {}".format(ip, n)
    p = subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE)
    output = str(p.communicate()[0])
    stats = output.split("\n")[-1].split(" = ")[-1].split("/")
    avg_ping_time = float(stats[1])  # In ms
    return avg_ping_time * 1000
