from pathlib import Path


def file_path(string):
    if Path(string).is_file():
        return Path(string)
    else:
        raise NotADirectoryError(string)


def dir_path(string):
    if Path(string).is_dir():
        return Path(string)
    else:
        raise NotADirectoryError(string)
