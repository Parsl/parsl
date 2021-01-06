import parsl
from pathlib import Path

# print((Path(__file__).parent / 'parsl_upload_complete.txt').resolve())

print(parsl.__path__)

with open((Path(__file__).parent / 'example.txt').resolve(), 'w+') as f:
    f.write('This is a test\n')
