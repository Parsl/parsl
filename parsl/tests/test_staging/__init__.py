def read_sort_write(in_path, out_path):
    with open(in_path) as u:
        strs = u.read().split()
        strs.sort()
    with open(out_path, 'w') as s:
        for e in strs:
            print(e, file=s)
