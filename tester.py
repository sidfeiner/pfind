import math
import os
import random
import logging
import re
import shutil
import stat
import subprocess
from datetime import datetime
import shlex
import sys
import platform
from collections import Counter
from pathlib import Path
from typing import List, Dict

DEBUG_LOG_FILE = './test-debug.log'
IGNORE_DEBUG_PRINTS = True if '--ignore-debug-prints' in sys.argv else False
IS_HARD = True if '--hard' in sys.argv else False
logging.basicConfig(format="%(msg)s", stream=sys.stdout, level='DEBUG' if '--debug' in sys.argv else 'INFO')

debug_logger = logging.getLogger("Debugger")
file_handler =logging.FileHandler(DEBUG_LOG_FILE, mode='w')
file_handler.setFormatter(logging.Formatter("%(asctime)s : %(levelname)s : %(message)s"))
debug_logger.addHandler(file_handler)
debug_logger.setLevel(logging.DEBUG)
debug_logger.propagate = False

TIMEOUT_SECONDS = 20
PFIND_EXEC = "./pfind"
TEST_DIR = "test_filesystem"
MAX_WORD_SIZE = 10
MAX_DEPTH = 30
MAX_DIRS_AMOUNT = 150
MAX_MATCHES_AMOUNT = 50
MAX_SEARCH_TERM_SIZE = 10
REGULAR_FILE_PROBA = 0.7  # When using links, 30% will be links and 70% regular files
UNSEARCHABLE_DIR_PROBA = 0.1
DEBUG = True
PERMISSION_DENIED_REGEX = re.compile(r"Directory (?P<path>.+): Permission denied\.")


def build_valid_chars():
    """Creates every letter 5 times to ensure a bigger probability of getting a letter, than a special char"""
    valid_chars = ""
    for i in range(ord('a'), ord('z')):
        valid_chars += 5 * chr(i)
    valid_chars += "-_."
    return valid_chars


valid_file_chars = build_valid_chars()


def parallelism_generator():
    for _ in range(10):
        yield 1
    for _ in range(150 if IS_HARD else 50):
        yield 2
    for i in range(10, 100, 10 if IS_HARD else 25):
        yield i
    for i in range(100, 1000, 50 if IS_HARD else 150):
        yield i
    for i in range(1000, 4001, 100 if IS_HARD else 500):
        yield i


def generate_word(size):
    """Generates a word that cannot be only dots"""
    s = ""
    for _ in range(size):
        s += random.choice(valid_file_chars)
    if s.count('.') > 0 and len(s.strip('.')) == 0:
        return generate_word(size)
    return s


def generate_dir(depth: int) -> str:
    parts = []
    for _ in range(depth):
        part = generate_word(random.randint(1, MAX_WORD_SIZE))
        while part in ['.', '..']:
            part = generate_word(random.randint(1, MAX_WORD_SIZE))
        parts.append(part)
    return os.path.join(*parts)


def generate_link_target(match_files: List[str], unmatched_files: List[str]):
    if 0 <= random.random() <= 0.5 and len(match_files) > 0:
        # To match
        if 0 <= random.random() <= 0.5:
            # To match file
            target, is_dir = random.choice(match_files), False
        else:
            # To match dir
            target, is_dir = os.path.dirname(random.choice(match_files)), True
    elif len(unmatched_files) > 0:
        # To unmatch
        if 0 <= random.random() <= 0.5:
            # To unmatch file
            target, is_dir = random.choice(unmatched_files), False
        else:
            # To unmatch dir
            target, is_dir = os.path.dirname(random.choice(unmatched_files)), True
    else:
        # Whatever
        if 0 <= random.random() <= 0.5:
            # To unmatch file
            target, is_dir = random.choice(unmatched_files + match_files), False
        else:
            # To unmatch dir
            target, is_dir = os.path.dirname(random.choice(unmatched_files + match_files)), True
    return target, is_dir


def get_all_dir_parents(dir_name: str):
    parents = []
    while dir_name != '':
        parent_name = os.path.dirname(dir_name)
        if parent_name != '':
            parents.append(parent_name)
        dir_name = parent_name
    return parents


def generate_dir_names(max_leafs_amt: int, with_parents: bool, exclude_dirs: List[str] = None):
    exclude_dirs = exclude_dirs or []
    s = set()
    for _ in range(max_leafs_amt):
        dir_name = os.path.join(TEST_DIR, generate_dir(random.randint(1, MAX_DEPTH)))
        while dir_name in exclude_dirs or dir_name in s:
            dir_name = os.path.join(TEST_DIR, generate_dir(random.randint(1, MAX_DEPTH)))
        if with_parents:
            all_parents = get_all_dir_parents(dir_name)
            for parent in all_parents:
                debug_logger.debug(f"added {parent} to dir names")
                s.add(parent)
        else:
            debug_logger.debug(f"added {dir_name} to dir names")
            s.add(dir_name)

    return list(s)


def generate_containing_word(file_dir: str, search_term: str):
    first_part_size = random.randint(0, MAX_WORD_SIZE // 2)
    second_part_size = random.randint(0, MAX_WORD_SIZE // 2)
    file_name = f"{generate_word(first_part_size)}{search_term}{generate_word(second_part_size)}"
    return os.path.join(file_dir, file_name)


def ensure_file_dir(path: str):
    """Receive path to file and ensure it's directory exists"""
    dir_name = os.path.dirname(path)
    debug_logger.debug(f"making dir {dir_name}")
    dir_path = Path(dir_name)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path


def touch_file(path: str):
    """Touches file and returns the directory it's in, as a Path"""
    dir_path = ensure_file_dir(path)
    debug_logger.debug(f"touching {path}")
    Path(path).touch()
    return dir_path


def ensure_generate_filesystem(match_files_amt: int, search_term: str, with_link: bool, with_unsearchable_dir: bool,
                               max_retries: int = 5):
    for _ in range(max_retries):
        try:
            return generate_filesystem(match_files_amt, search_term, with_link, with_unsearchable_dir)
        except Exception as e:
            logging.warning("failed creating file system, retrying (don't worry, this is not your fault).")
            reset_test_dir()
    raise RuntimeError("Tester failed generating file system. This is NOT your fault! Just re-run the tester")


def generate_filesystem(match_files_amt: int, search_term: str, with_link: bool, with_unsearchable_dir: bool):
    files_amount = random.randint(match_files_amt, match_files_amt + 4000)
    max_dirs_amount = random.randint(2, MAX_DIRS_AMOUNT)
    unsearchable_dirs_amt = max(1, math.ceil(UNSEARCHABLE_DIR_PROBA * max_dirs_amount)) if with_unsearchable_dir else 0
    dir_names = generate_dir_names(max_dirs_amount, True) + [TEST_DIR]
    unsearchable_dirs = []
    match_files = []
    match_links = []
    unmatched_files = []
    unmatched_links = []
    matchable_in_unsearchable_files = []

    def file_path_exists(_file_path: str):
        return not (
                _file_path in dir_names or _file_path in match_files or _file_path in match_links or _file_path in unmatched_files or _file_path in unmatched_links or _file_path in unsearchable_dirs or _file_path in matchable_in_unsearchable_files)

    regular_file_proba = REGULAR_FILE_PROBA if with_link else 1
    logging.info(f"creating {match_files_amt} files that should match")
    for _ in range(match_files_amt):
        file_dir = random.choice(dir_names)
        debug_logger.debug(f"picked random directory {file_dir}")

        file_path = generate_containing_word(file_dir, search_term)
        while not file_path_exists(file_path):
            file_path = generate_containing_word(file_dir, search_term)
        if not with_link or 0 <= random.random() <= regular_file_proba:
            debug_logger.debug(f"added {file_path} to match files")
            match_files.append(file_path)
        else:
            debug_logger.debug(f"added {file_path} to match links")
            match_links.append(file_path)

    logging.info(f"creating {files_amount - match_files_amt} files that will NOT match")
    for _ in range(files_amount - match_files_amt):
        file_dir = random.choice(dir_names)
        debug_logger.debug(f"picked random directory {file_dir}")
        # File generated MUSTN'T contain search term
        file_name = generate_word(random.randint(1, MAX_WORD_SIZE))
        final_path = os.path.join(os.path.join(file_dir, file_name))
        while search_term in file_name or not file_path_exists(final_path):
            file_name = generate_word(random.randint(1, MAX_WORD_SIZE))
            final_path = os.path.join(os.path.join(file_dir, file_name))
        if not with_link or 0 <= random.random() <= regular_file_proba:
            debug_logger.debug(f"added {final_path} to unmatch files")
            unmatched_files.append(final_path)
        else:
            debug_logger.debug(f"added {final_path} to unmatch links")
            unmatched_links.append(final_path)

    logging.info(
        f"creating {unsearchable_dirs_amt} directories without reading permissions (and adding matchable files inside)")
    unsearchable_dirs = generate_dir_names(unsearchable_dirs_amt, False, exclude_dirs=dir_names)
    dir_names.extend({parent for d in unsearchable_dirs for parent in get_all_dir_parents(d)})
    dir_names = list(set(dir_names))
    for unsearchable_dir in unsearchable_dirs:
        file_name = generate_word(random.randint(1, MAX_WORD_SIZE))
        final_path = os.path.join(os.path.join(unsearchable_dir, file_name))
        while search_term in file_name or not file_path_exists(final_path):
            file_name = generate_word(random.randint(1, MAX_WORD_SIZE))
            final_path = os.path.join(os.path.join(unsearchable_dir, file_name))
        debug_logger.debug(f"added {final_path} to matchable_in_unsearchable_files")
        matchable_in_unsearchable_files.append(final_path)

    logging.info("generating file system...")
    logging.info("generating files...")
    for p in match_files + unmatched_files:
        touch_file(p)
    if with_link:
        logging.info("generating links...")
        for p in match_links + unmatched_links:
            debug_logger.debug(f"ensuring dir {p}...")
            ensure_file_dir(p)
            target, is_dir = generate_link_target(match_files, unmatched_files)
            debug_logger.debug(f"creating symlink from {p} to {target}...")
            Path(p).symlink_to(target, target_is_directory=is_dir)
    if with_unsearchable_dir:
        logging.info("generating unsearchable dirs...")
        for p in matchable_in_unsearchable_files:
            dir_path = touch_file(p)
            debug_logger.debug(f"remove read permission from {p}")
            cur_permission = stat.S_IMODE(os.lstat(dir_path).st_mode)
            debug_logger.debug(f"permission {dir_path} before: {cur_permission}")
            dir_path.chmod(cur_permission & ~stat.S_IRUSR)
            cur_permission = stat.S_IMODE(os.lstat(dir_path).st_mode)
            debug_logger.debug(f"permission {dir_path} after: {cur_permission}")

    logging.info("done generating filesystem.")
    return match_files, match_links, unsearchable_dirs


def info_missing(lines: List[str], msg: str):
    if len(lines) > 0:
        logging.error('---------------')
        logging.error(msg)
        for f in lines:
            logging.error(f"{f}, with permission {stat.S_IMODE(os.lstat(f).st_mode)}")
        logging.error('---------------')


def info_missing_all(missing_files: List[str], missing_links: List[str], missing_unsearchable: List[str]):
    info_missing(missing_files, "Following files should have matched but weren't printed:")
    info_missing(missing_links, "Following links should have matched but weren't printed:")
    info_missing(missing_unsearchable,
                 "Following unsearchable files should have `Permission denied` but weren't printed:")


def find_duplicates(output: List[str]) -> (bool, Dict[str, int]):
    """Returns if duplicates were found and the duplicates"""
    c = Counter(output)
    duplicates = {k: v for k, v in c.items() if v > 1}
    return len(duplicates) > 0, duplicates


def info_redundant_prints(output: List[str], must_match_files: List[str], must_match_links: List[str],
                          unsearchable_dirs: List[str]):
    for line in output[:-1]:
        if 'Permission denied' in line:
            # Permission Denied
            match = PERMISSION_DENIED_REGEX.match(line)
            if match:
                path = match.group('path')
                if path not in unsearchable_dirs:
                    logging.error(f"pfind printed {path} as unsearchable, but it is searchable")
            else:
                logging.error("Permission denied error message was in wrong format")
        else:
            # Normal match
            if line not in must_match_files and line not in must_match_links:
                logging.error(f"pfind printed {line} as a match, but it shouldn't")


def assert_correct_results(must_match_files: List[str], must_match_links: List[str], unsearchable_dirs: List[str],
                           output: List[str], search_term: str, original_cmd: str):
    original_output = output
    output = [line for line in output[:-1] if '***' not in line] + [output[-1]] if IGNORE_DEBUG_PRINTS else output
    total_matches = len(must_match_files) + len(must_match_links) + len(unsearchable_dirs)
    has_duplicates, duplicates = find_duplicates(output)
    expected_last_line = f'Done searching, found {len(must_match_links) + len(must_match_files)} files'
    if len(output) != total_matches + 1 or has_duplicates or output[-1] != expected_last_line:
        logging.info("-------------- ERROR --------------")
        if output[-1] != expected_last_line:
            logging.error("#######")
            logging.error(f"Last line should have been `{expected_last_line}` but was `{output[-1]}`")
        if len(output) != total_matches + 1:
            logging.error("#######")
            logging.info(
                f"program did not print correct number of lines (matches_amount + permission_denied_amount + 1 = {total_matches + 1})")
            missing_files = [f for f in must_match_files if f not in output]
            missing_links = [f for f in must_match_links if f not in output]
            missing_unsearchable_files = [f for f in unsearchable_dirs if
                                          f"Directory {f}: Permission denied." not in output]
            info_missing_all(missing_files, missing_links, missing_unsearchable_files)
            info_redundant_prints(output, must_match_files, must_match_links, unsearchable_dirs)
        if has_duplicates:
            logging.error("#######")
            logging.error(
                f"Following lines have been printed more than once. First number is amount of times it was printed")
            for line, amt in duplicates.items():
                logging.error(f"{amt}: {line}")
        logging.error(
            f"for search term {search_term}, should have printed {total_matches + 1} lines but printed {len(output)}:")
        for line in original_output:
            logging.info(line)
        logging.error('----------------------------')
        logging.error("test file system has still not been deleted, so you can run the command by yourself to debug.")
        logging.error(f"command is: {original_cmd}")
        exit(1)


def reset_test_dir():
    logging.info("Resetting test directory")
    if os.path.exists(TEST_DIR):
        run_command(f"chmod -R u+r {TEST_DIR}")
        shutil.rmtree(TEST_DIR)
    os.mkdir(TEST_DIR)


def test_case(with_link: bool, with_unsearchable_dir: bool, timeout: int):
    tests_amt = 0
    failed_amt = 0
    match_files_amt = random.randint(3, MAX_MATCHES_AMOUNT)
    search_term = generate_word(random.randint(3, MAX_SEARCH_TERM_SIZE))
    logging.info(
        f"Generating file system with {match_files_amt} files that must be matched for search term {search_term}")
    reset_test_dir()

    match_files, match_links, unsearchable_dirs = ensure_generate_filesystem(match_files_amt, search_term, with_link,
                                                                             with_unsearchable_dir)
    logging.info("running on file system with many different parallelisms")
    for parallelism in parallelism_generator():
        tests_amt += 1
        cmd = f"""{PFIND_EXEC} {TEST_DIR} "{search_term}" {parallelism}"""
        success, output = run_command(cmd, timeout)
        if not success:
            logging.warning(f"!!!!!!!!!! WARNING !!!!!!!!!!")
            logging.warning(f"code returned non-zero exit code")
            failed_amt += 1
        assert_correct_results(match_files, match_links, unsearchable_dirs, output, search_term, cmd)
    logging.info("done running on file system")
    return tests_amt, failed_amt


def test_normal_run(timeout: int):
    logging.info("------------------------")
    logging.info("Normal test run")
    logging.info("------------------------")
    return test_case(False, False, timeout)


def test_links_run(timeout: int):
    logging.info("------------------------")
    logging.info("Links test run")
    logging.info("------------------------")
    return test_case(True, False, timeout)


def test_unsearchable_dir_run(timeout: int):
    logging.info("------------------------")
    logging.info("Unsearchable dir test run")
    logging.info("------------------------")
    return test_case(False, True, timeout)


def test_all(timeout: int):
    logging.info("------------------------")
    logging.info("Links and unsearchable dir test run")
    logging.info("------------------------")
    return test_case(True, True, timeout)


def test_non_existing_dir():
    logging.info("using pfind with non-existing directory")
    success, output = run_command(f"{PFIND_EXEC} /path/to/bad/dir search_term 1")
    if success:
        logging.info(f"Should return exit code other than 0 when root dir does not exist")
        exit(0)
    logging.info("successfully exited with non-zero exit code")


def test_file_as_root_dir():
    logging.info("creating temporary file to be used as root dir")
    path = Path('./test-file')
    path.touch()
    success, output = run_command(f"{PFIND_EXEC} {path.absolute()} search_term 1")
    if success:
        logging.info(f"Should return exit code other than 0 when root dir is a file and not a dir")
        path.unlink()
        exit(0)
    path.unlink()
    logging.info("successfully exited with non-zero exit code")


def test_bad_root_dir():
    logging.info("------------------------")
    logging.info("Test bad root dir as input")
    logging.info("------------------------")

    test_non_existing_dir()
    test_file_as_root_dir()


def run_command(command, timeout: int = TIMEOUT_SECONDS) -> (bool, List[str]):
    debug_logger.debug(f"running command: {command}")
    proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
    try:
        output, error = proc.communicate(timeout=timeout)
        if error is None and proc.returncode == 0:
            return True, output.strip().decode().split('\n')
        return False, output.strip().decode().split('\n')
    except subprocess.TimeoutExpired as e:
        proc.kill()
        output, error = proc.communicate()
        logging.error("--------------------------------------------")
        logging.error(
            f"Process did not finish in expected timeout (maximum {timeout} seconds) so you probably have a deadlock")
        logging.error("Anyway, this is the output you generated until the deadlock:")
        for line in output.strip().decode().split('\n'):
            print(line)
        logging.error(f"Test FileSystem won't be reset until the next time you run the test.")
        logging.error(f"So you can rerun the command by yourself to debug: {command}")
        exit(1)


def run_all_tests(timeout: int):
    logging.info("running...")
    test_bad_root_dir()

    tests_amt = 0
    failed_amt = 0
    for _ in range(30 if IS_HARD else 10):
        total_normal, failed_normal = test_normal_run(timeout)
        total_links, failed_links = test_links_run(timeout)
        total_unsearchable, failed_unsearchable = test_unsearchable_dir_run(timeout)
        total_all, failed_all = test_all(timeout)
        tests_amt += total_normal + total_links + total_unsearchable + total_all
        failed_amt += failed_normal + failed_links + failed_unsearchable + failed_all
    return tests_amt, failed_amt


def run(timeout_secs: int):
    logging.info("compiling...")
    compiler = "gcc-5.3.0" if 'nova' in platform.node() else 'gcc'
    success, output = run_command(
        f"{compiler} -O3 -D_POSIX_C_SOURCE=200809 -Wall -std=c11 -pthread pfind.c -o {PFIND_EXEC}", timeout)
    if not success:
        logging.error("compile unsuccessfull, output:")
        for line in output:
            print(line)
        exit(1)

    start_ts = datetime.now()
    tests_amt, failed_amt = run_all_tests(timeout_secs)
    end_ts = datetime.now()

    if failed_amt > 0:
        logging.warning(f"!!!!!!WARNING!!!!!!")
        logging.warning(f"{failed_amt}/{tests_amt} ended with exit code different than 1")
        logging.warning(f"But if your code got here, at least that means that the output was always correct")
    logging.info("WHO DA BEST??!! YOU DA BEST!!!! ")
    logging.info("You passed the tests, kululululululu")
    logging.info(
        f"And by the way, it took {(end_ts - start_ts).total_seconds()} seconds to run, if you want to compare zragim with others")
    logging.info("But remember this tester is random so comparing time isn't very effective")


if __name__ == '__main__':
    timeout = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isnumeric() else TIMEOUT_SECONDS
    run(timeout)
