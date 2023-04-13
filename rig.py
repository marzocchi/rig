#!/usr/bin/env python -u

# MIT License
#
# Copyright (c) 2020 Federico Marzocchi
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import argparse
import enum
import logging
import os
import queue
import shlex
import shutil
import signal
import subprocess
import sys
import threading
from argparse import REMAINDER
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from io import BufferedReader, StringIO, TextIOWrapper
from subprocess import DEVNULL, PIPE, Popen
from tempfile import NamedTemporaryFile
from threading import Lock, RLock
from typing import Any, Callable, Dict, Generator, IO, List, Optional, Protocol, cast

if os.getenv('RIG_DEBUG', '0') == '1':
    logging.getLogger().setLevel(logging.DEBUG)

try:
    from termcolor import colored
except ImportError:
    colored = lambda a, b: a

ColorizeFunc = Callable[[str], str]
ColorizeIterator = Generator[ColorizeFunc, None, None]


def main():
    program_factory = ProgramFactory(create_colorize_iterator())

    cli = create_argparser()

    args = cli.parse_args()

    programs = []

    try:
        rigfile = read_rigfile(args.rigfile[0], args.rigfile_args)
    except RigfileError as e:
        if e.stderr != "":
            message = f"can't read '{e.rigfile_name}': {e.message}\nstderr:\n{e.stderr}"
        else:
            message = f"can't read '{e.rigfile_name}': {e.message}"

        print_error(message=message)
        sys.exit(1)

    try:
        programs = program_factory.from_rigfile(rigfile)
    except CommandLineError as e:
        print_error(message=f"can't parse '{e.rigfile_name}': {e.message} on line {e.line}")

    stdin = buffer_stdin(sys.stdin)
    write_lock = threading.RLock()

    events = cast(EventsQueue, queue.Queue())

    register_term_signal_handlers(events)

    label_size = max([len(p.name) for p in programs])
    pad_label = lambda s: s.ljust(label_size)

    stream_controller = StreamController(events, cast(ColorizeFunc, partial(colorize, 'white')), pad_label=pad_label)

    controller_factories: Dict[str, Callable[[], Controller]] = {
        'stream': lambda: stream_controller,
        'report': lambda: ReportController(stream_controller)
    }

    controller = controller_factories[args.command]()

    with ThreadPoolExecutor(max_workers=len(programs) + 1) as executor:
        program_futures = []

        monitor_events_future = executor.submit(monitor_events, controller, events, len(programs))

        for program in programs:
            program_futures.append(executor.submit(
                controller.execute,
                write_lock,
                sys.stdout,
                sys.stderr,
                sys.stderr,
                program,
                get_stdin(stdin)
            ))

        completed_programs = [f.result() for f in program_futures]
        skipped_programs = [cp for cp in completed_programs if cp is None]
        failed_programs = [cp for cp in completed_programs if cp is not None and cp.exit_code != 0]

        monitor_events_future.result()

    exit_code = 0 if len(failed_programs) == 0 and len(skipped_programs) == 0 else 1

    if len(failed_programs) > 0:
        failed_program_names = [p.program.name for p in failed_programs]
        if len(failed_programs) == len(programs):
            print_error(message="all programs failed.")
        else:
            print_error(message=f"some programs failed: {failed_program_names}.")

    sys.exit(exit_code)


def print_error(message: str):
    sys.stderr.write(f"{message}\n")


def create_argparser():
    cli = argparse.ArgumentParser()

    sub = cli.add_subparsers(dest="command", title="commands", required=True)

    sub_stream = sub.add_parser("stream")
    sub_report = sub.add_parser("report")

    sub_stream.add_argument('rigfile', nargs=1)
    sub_stream.add_argument('rigfile_args', nargs=REMAINDER)

    sub_report.add_argument('rigfile', nargs=1)
    sub_report.add_argument('rigfile_args', nargs=REMAINDER)

    return cli


def get_stdin(stdin: Optional[NamedTemporaryFile]) -> Optional[IO]:
    if stdin is None:
        return None

    return open(stdin.name, 'r')


@dataclass(frozen=True)
class Event:
    @dataclass(frozen=True)
    class Type(enum.Enum):
        EXECUTION_STARTED = 'EXECUTION_STARTED'
        EXECUTION_COMPLETED = 'EXECUTION_COMPLETED'
        EXECUTION_SKIPPED = 'EXECUTION_SKIPPED'
        TERMINATED = 'SIGNAL'

        def __eq__(self, o: object) -> bool:
            if not isinstance(o, Event.Type):
                return False

            return o.value == self.value

    type: Type
    payload: Optional[Any] = None


class WriteEventQueue(Protocol):
    def put(self, e: Event):
        pass


class EventsQueue(WriteEventQueue):
    def get(self) -> Event:
        pass


def monitor_events(controller: 'Controller', events: EventsQueue, programs_count: int) -> int:
    running_processes: List[Popen] = []

    while True:
        event = events.get()
        logging.debug(f"event={event.type}, payload={event.payload}")

        if event.type == Event.Type.EXECUTION_STARTED:
            running_processes.append(event.payload)

        elif event.type in [Event.Type.EXECUTION_COMPLETED, Event.Type.EXECUTION_SKIPPED]:
            if event.type == Event.Type.EXECUTION_COMPLETED:
                running_processes = [p for p in running_processes if p != event.payload]

            programs_count -= 1

            if programs_count == 0:
                logging.debug(f"no more programs to wait for")
                break

        elif event.type == Event.Type.TERMINATED:
            for p in running_processes:
                p.send_signal(event.payload)

            controller.shutdown()


def register_term_signal_handlers(events: EventsQueue):
    def handler(s, frame):
        events.put(Event(type=Event.Type.TERMINATED, payload=s))

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


@dataclass(frozen=True)
class Rigfile:
    name: str
    source: StringIO


@dataclass(frozen=True)
class Program:
    colorizer: ColorizeFunc
    rigfile: Rigfile
    name: str
    command_line: List[str]


@dataclass(frozen=True)
class RigfileError(Exception):
    rigfile_name: str
    message: str
    stderr: str = ""


@dataclass(frozen=True)
class CommandLineError(Exception):
    rigfile_name: str
    message: str
    line: int


def read_rigfile(rigfile_name: str, args: List[str]) -> Rigfile:
    executable = rigfile_name.endswith('.sh')

    if executable:
        try:
            command = [rigfile_name]
            command.extend(args)
            result = subprocess.run(command, capture_output=True)
        except PermissionError as e:
            raise RigfileError(rigfile_name=rigfile_name, message=f"not executable") from e
        except OSError as e:
            raise RigfileError(rigfile_name=rigfile_name, message=str(e)) from e

        if result.returncode > 0:
            raise RigfileError(
                rigfile_name=rigfile_name,
                message=f"execution failed (exit code={-1 * result.returncode})",
                stderr=result.stderr.decode('UTF-8'),
            )

        source = result.stdout.decode('UTF-8').strip()
        if source == "":
            raise RigfileError(rigfile_name=rigfile_name,
                               message=f"executing {rigfile_name} with args {args} produced no output")

        source = StringIO(source)
        for i, line in enumerate(source):
            logging.debug(f"{rigfile_name},{i + 1}: {line.strip()}")

        source.seek(0)

        return Rigfile(name=rigfile_name, source=source)
    else:
        try:
            rigfile = open(rigfile_name, 'r')
        except FileNotFoundError as e:
            raise RigfileError(rigfile_name=rigfile_name, message=str(e)) from e

        source = rigfile.read().strip()

        if source == "":
            raise RigfileError(rigfile_name=rigfile_name, message=f"{rigfile_name} is empty")

        return Rigfile(name=rigfile_name, source=StringIO(source))


def buffer_stdin(stdin: IO) -> Optional[IO]:
    if stdin.isatty():
        return None

    temp_file = NamedTemporaryFile(delete=False)
    temp_file.write(sys.stdin.read().encode())
    temp_file.flush()
    return temp_file


def colorize(color: str, txt: str):
    return colored(txt, color)


def create_colorize_iterator() -> ColorizeIterator:
    colors = ['green', 'red', 'yellow', 'blue', 'cyan', 'magenta']

    i = 0
    while True:
        try:
            color = colors[i]
            i += 1
        except IndexError:
            color = colors[0]
            i = 0

        yield partial(colorize, color)


class ProgramFactory:
    def __init__(self, colors: ColorizeIterator):
        self._colors = colors
        self.__counters = defaultdict(lambda: 1)

    def from_rigfile(self, rigfile: Rigfile) -> List[Program]:
        programs = []

        for i, line in enumerate(rigfile.source):
            try:
                words = shlex.split(line)
            except ValueError as e:
                raise CommandLineError(line=i + 1, message=f"{e}", rigfile_name=rigfile.name) from e

            if len(words) == 0:
                continue

            if len(words) < 2:
                raise CommandLineError(line=i + 1,
                                       message="a command line must have at least a name and a command",
                                       rigfile_name=rigfile.name)

            base_name = words.pop(0)

            if self.__counters[base_name] < 2:
                name = base_name
            else:
                name = f"{base_name}#{self.__counters[base_name]}"

            programs.append(
                Program(name=name, command_line=words, rigfile=rigfile, colorizer=self._colors.__next__()))
            self.__counters[base_name] += 1

        return programs


@dataclass(frozen=True)
class CompletedProgram:
    program: Program
    exit_code: int


class Controller(Protocol):
    def shutdown(self):
        pass

    def execute(self, write_lock: RLock, stdout: IO, stderr: IO, alerts: IO, p: Program,
                stdin: Optional[IO]) -> Optional[CompletedProgram]:
        pass


class StreamController(Controller):
    def __init__(self, events: WriteEventQueue, message_colorizer: ColorizeFunc, pad_label: Callable[[str], str]):
        self.__events = events
        self.__message_colorizer = message_colorizer
        self.__pad_label = pad_label
        self.__lock = Lock()
        self.__shutdown = False

    @staticmethod
    def _write_message(write_lock: RLock, dst: IO, line: str, prefix: str, colorize: ColorizeFunc):
        with write_lock:
            dst.write(colorize(prefix))
            dst.write(" ")
            dst.write(line + "\n")

    @staticmethod
    def _copy(write_lock: RLock, dst: TextIOWrapper, src: BufferedReader, prefix: str, colorize: ColorizeFunc):
        while True:
            line = src.readline()
            if line is None or len(line) == 0:
                break

            with write_lock:
                dst.write(colorize(prefix))
                dst.write(" ")
                dst.write(line.decode('UTF-8'))

    def shutdown(self):
        with self.__lock:
            self.__shutdown = True

    def __did_shutdown(self) -> bool:
        with self.__lock:
            return self.__shutdown

    def execute(self, write_lock: RLock, stdout: IO, stderr: IO, alerts: IO, p: Program,
                stdin: Optional[IO]) -> Optional[CompletedProgram]:

        message_prefix = self.__pad_label(p.name)
        colorize_message = self.__message_colorizer

        if self.__did_shutdown():
            self._write_message(write_lock, alerts, "will not start", message_prefix, colorize_message)
            self.__events.put(Event(type=Event.Type.EXECUTION_SKIPPED))
            return

        started_at = datetime.now()

        stdin = stdin if stdin is not None else DEVNULL

        try:
            proc = Popen(p.command_line, stdin=stdin, stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid)
        except OSError as e:
            self._write_message(write_lock, alerts, f"{e}", message_prefix, colorize_message)
            self.__events.put(Event(type=Event.Type.EXECUTION_SKIPPED))
            return CompletedProgram(program=p, exit_code=127)

        self.__events.put(Event(type=Event.Type.EXECUTION_STARTED, payload=proc))

        with proc as proc:
            self._write_message(write_lock, alerts, f"running with PID {proc.pid}", message_prefix, colorize_message)

            futures = []

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures.append(executor.submit(self._copy, write_lock, stdout, proc.stdout, self.__pad_label(p.name),
                                               p.colorizer))

                futures.append(executor.submit(self._copy, write_lock, stderr, proc.stderr, self.__pad_label(p.name),
                                               p.colorizer))

                exit_code = proc.wait()
                time_elapsed = datetime.now() - started_at

                self.__events.put(Event(type=Event.Type.EXECUTION_COMPLETED, payload=proc))

            for f in futures:
                f.result()

        self._write_message(write_lock, stderr,
                            f"done in {time_elapsed.total_seconds():.2f}s with exit code {-1 * exit_code}",
                            message_prefix, colorize_message)

        return CompletedProgram(program=p, exit_code=exit_code)


class ReportController(Controller):
    def __init__(self, stream_controller: StreamController):
        self.__stream_controller = stream_controller

    def execute(self, write_lock: RLock, stdout: IO, stderr: IO, alerts: IO, p: Program,
                stdin: Optional[IO]) -> Optional[CompletedProgram]:
        stdout_buffer = NamedTemporaryFile(mode='w+')
        stderr_buffer = NamedTemporaryFile(mode='w+')

        completed_program = self.__stream_controller.execute(write_lock, stdout_buffer, stderr_buffer, alerts, p, stdin)

        stdout_buffer.seek(0)
        stderr_buffer.seek(0)

        with write_lock:
            shutil.copyfileobj(stdout_buffer, stdout)
            shutil.copyfileobj(stderr_buffer, stderr)

        return completed_program


if __name__ == "__main__":
    main()
