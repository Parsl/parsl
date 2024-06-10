from parsl.data_provider.files import File
from parsl.dataflow.memoization import id_for_memo


@id_for_memo.register(File)
def id_for_memo_file(file: File, output_ref: bool = False):
    return file.url
