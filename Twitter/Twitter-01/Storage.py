
import re
from pathlib import Path


class Storage:
    """Storage class"""

    # class variable
    _root_path = Path('C:\\DSTI\\CA Project Technical\\data\\twitter')

    def __init__(self, dataset, basename, extension='txt', max_size=100 * 1024 * 1024):
        self.data_set_dir = type(self)._root_path / dataset
        self.data_set_dir.mkdir(exist_ok=True)
        self.basename = basename
        self.extension = extension
        self.max_size = max_size
        self.data_file_format = '{}-{{:03d}}.{}'.format(basename, extension)
        self.data_file_pattern = re.compile('{}-(\d+)\.{}$'.format(basename, extension))

    def get_data_file_index(self, data_file):
        file_index = None
        data_file_result = self.data_file_pattern.match(data_file.name)
        if data_file_result:
            file_index = data_file_result.group(1).lstrip('0')
            if file_index == '':
                file_index = 0
            else:
                file_index = int(file_index)

        return file_index

    def get_data_file_from_index(self, file_index):
        return self.data_set_dir / self.data_file_format.format(file_index)

    def get_all_data_files(self):
        tmp = dict()
        for data_file in [x for x in self.data_set_dir.iterdir() if x.is_file()]:
            file_index = self.get_data_file_index(data_file)
            if file_index is not None:
                tmp[file_index] = data_file

        data_files = list()
        for key in sorted(tmp.keys()):
            data_files.append(tmp[key])

        return data_files

    def get_current_data_file(self):
        data_file = self.get_data_file_from_index(0)
        remaining_size = self.max_size
        data_files = self.get_all_data_files()
        if len(data_files) > 0:
            data_file = data_files[-1]
            if data_file.stat().st_size < self.max_size:
                remaining_size = self.max_size - data_file.stat().st_size
            else:
                file_index = self.get_data_file_index(data_file) + 1
                data_file = self.get_data_file_from_index(file_index)

        return data_file, remaining_size

    def add_file_to_storage(self, input_file_path):
        data_file = None
        remaining_size = 0
        with open(input_file_path, 'r') as input_file:
            for line in input_file:
                if data_file is None:
                    data_file, remaining_size = self.get_current_data_file()
                    file = data_file.open(mode='a')
                remaining_size -= file.write(line)
                if remaining_size < 0:
                    file.close()
                    data_file = None
        if data_file is not None:
            file.close()

    def is_empty(self):
        return len(self.get_all_data_files()) == 0

    def clear(self):
        for data_file in self.get_all_data_files():
            # unreliable: unlink() raises exceptions when file is being used (e.g. background process)
            data_file.unlink()


def test_storage():
    data_dir = 'testdir'
    basename = 'testfile'
    storage = Storage(data_dir, basename, max_size=20)

    for index in range(0, 1000):
        data_file = storage.get_data_file_from_index(index)
        file_index = storage.get_data_file_index(data_file)
        if file_index != index:
            print('ERROR : expected index: {}, retrieved: {} for data file {}'.format(index, file_index, data_file))

    print(storage.get_all_data_files())
    data_file, remaining_size = storage.get_current_data_file()
    print(data_file, remaining_size)

    data_file.write_text('1234567890')
    print(storage.get_all_data_files())
    data_file, remaining_size = storage.get_current_data_file()
    print(data_file, remaining_size)

    data_file, remaining_size = storage.get_current_data_file()
    data_file.write_text('abcdefghijklmnopqrstuvwxyz')

    data_file, remaining_size = storage.get_current_data_file()
    data_file.write_text('abcdefghijklmnopqrstuvwxyz')

    data_file, remaining_size = storage.get_current_data_file()
    data_file.write_text('1234567890')
    print(storage.get_all_data_files())

    if True:
        for data_file in storage.get_all_data_files():
            data_file.unlink()
        storage.data_set_dir.rmdir()


if __name__ == '__main__':
    test_storage()
