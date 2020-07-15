#!/usr/bin/env python

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
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from io import BufferedReader, StringIO, TextIOWrapper
from subprocess import DEVNULL, PIPE, Popen
from tempfile import NamedTemporaryFile
from threading import RLock
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

    cli = argparse.ArgumentParser()
    cli.add_argument("mode", nargs=1, choices=['stream', 'report'])
    cli.add_argument('rigfile', nargs='+')

    args = cli.parse_args()

    rigfiles = []
    programs = []

    try:
        rigfiles = read_rigfiles(args.rigfile)
    except RigfileError as e:
        cli.error(message=f"can't read '{e.rigfile_name}': {e.message}")
        cli.exit(1)

    try:
        programs = program_factory.from_rigfiles(rigfiles)
    except CommandLineError as e:
        cli.error(message=f"can't parse '{e.rigfile_name}': {e.message} on line {e.line}")
        cli.exit(1)

    stdin = buffer_stdin(sys.stdin)
    write_lock = threading.RLock()

    events = queue.Queue()

    signal.signal(signal.SIGINT, get_signal_handler(events))
    signal.signal(signal.SIGTERM, get_signal_handler(events))

    label_size = max([len(p.name) for p in programs])
    pad_label = lambda s: s.ljust(label_size)

    stream_controller = StreamController(events, cast(ColorizeFunc, partial(colorize, 'white')), pad_label=pad_label)

    controller_factories: Dict[str, Callable[[], Controller]] = {
        'stream': lambda: stream_controller,
        'report': lambda: ReportController(stream_controller)
    }

    controller = controller_factories[args.mode[0]]()

    with ThreadPoolExecutor(max_workers=len(programs) + 1) as executor:
        program_futures = []

        monitor_events_future = executor.submit(monitor_events, events)

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
        failed_programs = [cp for cp in completed_programs if cp.exit_code != 0]

        events.put(Event(type=Event.Type.ALL_PROGRAMS_COMPLETED))
        monitor_events_future.result()

    exit_code = 0 if len(failed_programs) == 0 else 1
    cli.exit(exit_code)


def get_stdin(stdin: Optional[NamedTemporaryFile]) -> Optional[IO]:
    if stdin is None:
        return None

    return open(stdin.name, 'r')


@dataclass(frozen=True)
class Event:
    @dataclass(frozen=True)
    class Type(enum.Enum):
        PROGRAM_STARTED = 'PROGRAM_STARTED'
        PROGRAM_COMPLETED = 'PROGRAM_COMPLETED'
        ALL_PROGRAMS_COMPLETED = 'ALL_PROGRAMS_COMPLETED'
        INCOMING_SIGNAL = 'SIGNAL'

        def __eq__(self, o: object) -> bool:
            if not isinstance(o, Event.Type):
                return False

            return o.value == self.value

    type: Type
    payload: Optional[Any] = None


class EventsQueue(Protocol):
    def put(self, e: Event):
        pass

    def get(self) -> Event:
        pass


def monitor_events(events: EventsQueue):
    running_processes: List[Popen] = []

    while True:
        event = events.get()
        logging.debug(f"event={event.type}, payload={event.payload}")

        if event.type == Event.Type.PROGRAM_STARTED:
            running_processes.append(event.payload)

        elif event.type == Event.Type.PROGRAM_COMPLETED:
            running_processes = [p for p in running_processes if p != event.payload]
            if len(running_processes) == 0:
                events.put(Event(type=Event.Type.ALL_PROGRAMS_COMPLETED))

        elif event.type == Event.Type.INCOMING_SIGNAL:
            for p in running_processes:
                p.send_signal(event.payload)

        elif event.type == Event.Type.ALL_PROGRAMS_COMPLETED:
            break


def get_signal_handler(events: EventsQueue):
    def handler(s, frame):
        events.put(Event(type=Event.Type.INCOMING_SIGNAL, payload=s))

    return handler


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


@dataclass(frozen=True)
class CommandLineError(RigfileError):
    line: int


@dataclass(frozen=True)
class CompletedProgram:
    program: Program
    exit_code: int


def read_rigfiles(rigfile_names: List[str]) -> List[Rigfile]:
    executable_rigfiles = [rigfile for rigfile in rigfile_names if rigfile.endswith('.sh')]
    static_rigfiles = [rigfile for rigfile in rigfile_names if not rigfile.endswith('.sh')]

    rigfiles = []
    for rigfile_name in executable_rigfiles:
        try:
            logging.debug(f"executing rigfile {rigfile_name}")
            result = subprocess.run(rigfile_name, capture_output=True)
        except OSError as e:
            logging.debug(f"execution failed")
            raise RigfileError(rigfile_name=rigfile_name, message=str(e)) from e

        logging.debug(f"execution of rigfile {rigfile_name} successful")

        if result.returncode > 0:
            raise RigfileError(rigfile_name=rigfile_name,
                               message=f"execution failed (exit code={-1 * result.returncode})")

        source = result.stdout.decode('UTF-8').strip()
        if source == "":
            raise RigfileError(rigfile_name=rigfile_name, message=f"empty")

        source = StringIO(source)
        for i, line in enumerate(source):
            logging.debug(f"{rigfile_name},{i + 1}: {line.strip()}")

        source.seek(0)

        rigfiles.append(Rigfile(name=rigfile_name, source=source))

    for rigfile_name in static_rigfiles:
        try:
            rigfile = open(rigfile_name, 'r')
        except FileNotFoundError as e:
            raise RigfileError(rigfile_name=rigfile_name, message=str(e)) from e

        source = rigfile.read().strip()

        if source == "":
            raise RigfileError(rigfile_name=rigfile_name, message=f"empty")

        rigfiles.append(Rigfile(name=rigfile_name, source=StringIO(source)))

    return rigfiles


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

    def from_rigfiles(self, rigfiles: List[Rigfile]) -> List[Program]:
        programs = []

        for rigfile in rigfiles:
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
                name = f"{base_name}#{self.__counters[base_name]}"

                programs.append(
                    Program(name=name, command_line=words, rigfile=rigfile, colorizer=self._colors.__next__()))
                self.__counters[base_name] += 1

        return programs


class Controller(Protocol):
    def execute(self, write_lock: RLock, stdout: IO, stderr: IO, alerts: IO, p: Program,
                stdin: Optional[IO]) -> CompletedProgram:
        pass


class StreamController(Controller):
    def __init__(self, events: queue.Queue, alerts_colorizer: ColorizeFunc, pad_label: Callable[[str], str]):
        self.__events = events
        self.__colorize_alert = alerts_colorizer
        self.__pad_label = pad_label

    @staticmethod
    def _write_alert(write_lock: RLock, dst: IO, line: str, prefix: str, colorize: ColorizeFunc):
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

    def execute(self, write_lock: RLock, stdout: IO, stderr: IO, alerts: IO, p: Program,
                stdin: Optional[IO]) -> CompletedProgram:

        alert_prefix = self.__pad_label(p.name)
        alert_color = self.__colorize_alert

        started_at = datetime.now()

        stdin = stdin if stdin is not None else DEVNULL

        try:
            proc = Popen(p.command_line, stdin=stdin, stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid)
        except OSError as e:
            self._write_alert(write_lock, alerts, f"{e}", alert_prefix, alert_color)
            return CompletedProgram(program=p, exit_code=127)

        self.__events.put(Event(type=Event.Type.PROGRAM_STARTED, payload=proc))

        with proc as proc:
            self._write_alert(write_lock, alerts, f"running with PID {proc.pid}", alert_prefix, alert_color)

            futures = []

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures.append(executor.submit(self._copy, write_lock, stdout, proc.stdout, self.__pad_label(p.name),
                                               p.colorizer))
                futures.append(executor.submit(self._copy, write_lock, stderr, proc.stderr, self.__pad_label(p.name),
                                               p.colorizer))

                exit_code = proc.wait()

                time_elapsed = datetime.now() - started_at

                self.__events.put(Event(type=Event.Type.PROGRAM_COMPLETED, payload=proc))

            for f in futures:
                f.result()

        self._write_alert(write_lock, alerts,
                          f"done in {time_elapsed.total_seconds():.2f}s with exit code {-1 * exit_code}",
                          alert_prefix, alert_color)

        return CompletedProgram(program=p, exit_code=exit_code)


class ReportController(Controller):
    def __init__(self, stream_controller: StreamController):
        self.__stream_controller = stream_controller

    def execute(self, write_lock: RLock, stdout: IO, stderr: IO, alerts: IO, p: Program,
                stdin: Optional[IO]) -> CompletedProgram:
        stdout_buffer = NamedTemporaryFile(mode='w+')
        stderr_buffer = NamedTemporaryFile(mode='w+')

        p = self.__stream_controller.execute(write_lock, stdout_buffer, stderr_buffer, alerts, p, stdin)

        stdout_buffer.seek(0)
        stderr_buffer.seek(0)

        with write_lock:
            shutil.copyfileobj(stdout_buffer, stdout)
            shutil.copyfileobj(stderr_buffer, stderr)

        return p


if __name__ == "__main__":
    main()
