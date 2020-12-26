import os
import random
import re
import shutil
import subprocess
import shlex
import time
import platform
from pathlib import Path
from subprocess import PIPE, check_output
from typing import List

MAX_LOG_INTERVAL_SECONDS = 2
PFIND_EXEC = "./pfind"
TEST_DIR = "test"
MAX_WORD_SIZE = 10

valid_file_chars = ""
for i in range(ord('A'), ord('Z')):
    valid_file_chars += chr(i)
for i in range(ord('a'), ord('z')):
    valid_file_chars += chr(i)
valid_file_chars += "-_."


def parallelism_generator():
    yield 1
    yield 2
    for i in range(10, 100, 25):
        yield i
    for i in range(100, 1000, 150):
        yield i
    for i in range(1000, 4000, 500):
        yield i


def generate_word(size):
    s = ""
    for _ in range(size):
        s += random.choice(valid_file_chars)
    return s


def generate_dir(depth: int) -> str:
    parts = [generate_word(random.randint(1, MAX_WORD_SIZE)) for _ in range(depth)]
    return os.path.join(*parts)


def generate_filesystem(match_files_amt: int, search_term: str):
    files_amount = random.randint(match_files_amt, match_files_amt + 3000)
    max_dirs_amount = random.randint(1, 300)
    dir_names = [generate_dir(random.randint(1, MAX_WORD_SIZE)) for _ in range(max_dirs_amount)]
    match_files = []
    unmatched_files = []
    for _ in range(match_files_amt):
        file_dir = random.choice(dir_names)
        first_part_size = random.randint(0, MAX_WORD_SIZE // 2)
        second_part_size = random.randint(0, MAX_WORD_SIZE // 2)
        file_path = os.path.join(TEST_DIR, file_dir,
                                 f"{generate_word(first_part_size)}{search_term}{generate_word(second_part_size)}")
        match_files.append(file_path)
    for _ in range(files_amount - match_files_amt):
        file_dir = random.choice(dir_names)
        # File generated MUSTN'T contain search term
        file_name = generate_word(random.randint(1, MAX_WORD_SIZE))
        while search_term in file_name:
            file_name = generate_word(random.randint(1, MAX_WORD_SIZE))
        unmatched_files.append(os.path.join(TEST_DIR, file_dir, file_name))

    print("generating file system")
    for p in match_files + unmatched_files:
        dir_name = os.path.dirname(p)
        Path(dir_name).mkdir(parents=True, exist_ok=True)
        Path(p).touch(exist_ok=True)

    return match_files


def assert_correct_results(must_match_files: List[str], output: List[str], search_term: str):
    filtered_output = [line for line in output if '] : ' not in line]
    if len(filtered_output) != len(must_match_files) + 1:
        print(f"program did not print correct number of lines (matches_amount + 1 = {len(must_match_files) + 1})")
        missing_files = [f for f in must_match_files if f not in filtered_output]
        if len(missing_files) > 0:
            print("Following files should have matched but weren't printed:")
            for f in missing_files:
                print(f)
        print(
            f"for search term {search_term}, should have printed {len(must_match_files) + 1} lines but printed {len(filtered_output)}:")
        for line in output:
            print(line)
        exit(1)


def test_normal_run():
    shutil.rmtree(TEST_DIR, ignore_errors=True)
    match_files_amt = random.randint(3, 100)
    search_term = generate_word(random.randint(1, 10))
    match_files = generate_filesystem(match_files_amt, search_term)
    for parallelism in parallelism_generator():
        print(f"running pfind with parallelism {parallelism}")
        output = run_command(f"""{PFIND_EXEC} {TEST_DIR} "{search_term}" {parallelism}""")
        assert_correct_results(match_files, output, search_term)


def run_command(command):
    print(f"running command: {command}")
    process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    lines = []
    checkpoint = time.time()
    while True:
        output = process.stdout.readline().decode()
        if output == '' and (process.poll() is not None or (time.time() - checkpoint) >= MAX_LOG_INTERVAL_SECONDS):
            break
        if output:
            lines.append(output.strip())
            checkpoint = time.time()
    rc = process.poll()
    return lines


def run(root_dir: str, search_term: str, parallelism: int):
    print("compiling...")
    compiler = "gcc-5.3.0" if 'nova' in platform.node() else 'gcc'
    run_command(f"{compiler} -O3 -D_POSIX_C_SOURCE=200809 -Wall -std=c11 -pthread pfind.c -o {PFIND_EXEC}")
    print("running...")
    for _ in range(10):
        test_normal_run()


if __name__ == '__main__':
    run('/Users/sid/', 'pdf', 1)
