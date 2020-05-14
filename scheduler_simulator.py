#!/usr/bin/env python3
#
# Copyright (c) 2020, Maxime Piraux
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

"""
This module implements a simulator for an abstract multipath transport protocol.
This protocol transfers a file over several paths using a custom function.
Complex multipath topologies can be expressed and simulated, as illustrated at
the end of this module.
"""

from builtins import NotImplementedError
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Union, Tuple, Optional, Dict


class PacketType(Enum):
    DATA = 1
    ACK = 2
    RTX = 3
    FIN = 4


@dataclass
class Packet:
    length: int = 0
    type: PacketType = PacketType.DATA
    seq_number: int = 0

    def __hash__(self) -> int:
        return hash((self.seq_number, self.type, self.length))

    def log_str(self) -> str:
        names = {
            PacketType.DATA: 'DATA',
            PacketType.ACK: 'ACK',
            PacketType.RTX: 'RTX',
            PacketType.FIN: 'FIN',
        }

        if self.type == PacketType.DATA or self.type == PacketType.RTX:
            return f'{names[self.type]}[{self.seq_number}, {self.seq_number+self.length}]'
        elif self.type == PacketType.ACK:
            return f'{names[self.type]}[{self.packet_acknowledged.log_str()}]'
        elif self.type == PacketType.FIN:
            return f'{names[self.type]}[{self.seq_number}]'

        return 'UNKNOWN[]'


@dataclass
class Link:
    """ A link that can queue up 1.5*BDP and paces packets when dequeuing them. """
    bandwidth: int  # Bps
    delay: float  # s
    mtu: int  # bytes
    max_queue_size: int = field(init=False)  # bytes
    _next_dequeue_slot: float = field(init=False, default=0)  # s
    _pacing_time: float = field(init=False)  # s
    _queue: List[Tuple[Packet, float]] = field(init=False, default_factory=list)
    _queue_size: int = 0

    def __post_init__(self):
        self.max_queue_size = int(self.bandwidth * 2 * self.delay * 1.5)
        self._pacing_time = 1 / int(self.bandwidth / self.mtu)

    def next_free_slot_time(self, time: float) -> float:
        if not self._queue:
            return time
        p, queuing_time = self._queue[0]
        return max(queuing_time + self.delay, time)

    def enqueue(self, p: Packet, time: float) -> Union[bool, float]:
        if p.length > self.mtu:
            return float('inf')
        if self._queue_size + p.length <= self.max_queue_size:
            self._queue.append((p, time))
            self._queue_size += p.length
            return True
        return self.next_free_slot_time(time)

    def dequeue(self, time: float) -> Union[Packet, float]:
        """ Returns the next packet or the time at which the next packet may be dequeued. """
        if not self._queue:
            return time + self.delay
        if time < self._next_dequeue_slot:
            return self._next_dequeue_slot

        p, queuing_time = self._queue[0]
        if queuing_time + self.delay <= time:
            self._queue = self._queue[1:]
            self._queue_size -= p.length
            self._next_dequeue_slot = time + (self._pacing_time if self._queue and self._queue[0][0].length > 0 else 0)
            return p
        return max(queuing_time + self.delay, time)


@dataclass
class Interface:
    sending_link: Link
    receiving_link: Link

    def send(self, p: Packet, time: float) -> Union[bool, float]:
        """ Returns whether the packet was accepted and the next time at which a packet may be accepted. """
        return self.sending_link.enqueue(p, time)

    def receive(self, time: float) -> Union[Packet, float]:
        """ Returns the first packet received or the next time at which a packet may be received. """
        return self.receiving_link.dequeue(time)


class CCState(Enum):
    slow_start = 'slow_start'
    congestion_avoidance = 'congestion_avoidance'
    recovery = 'recovery'


class CongestionController:
    state: CCState = CCState.slow_start

    def blocked(self, packet_len: int) -> bool:
        raise NotImplementedError

    def packet_sent(self, p: Packet, time: float):
        raise NotImplementedError

    def packet_acknowledged(self, p: Packet, time: float):
        raise NotImplementedError

    def packet_dupack(self, time: float):
        raise NotImplementedError

    def packet_lost(self, p: Packet, time: float):
        raise NotImplementedError


@dataclass
class NewReno(CongestionController):
    mss: int  # bytes
    bytes_in_flight: int = 0  # bytes
    ssthresh: int = 1000_000_000  # bytes
    cwin: int = 0  # bytes
    dupacks: int = 0  # packets

    def __post_init__(self):
        self.cwin = self.mss

    def blocked(self, packet_len: int) -> bool:
        return packet_len > self.mss or self.bytes_in_flight + packet_len > self.cwin

    def packet_sent(self, p: Packet, time: float):
        self.bytes_in_flight += p.length

    def packet_acknowledged(self, p: Packet, time: float):
        self.dupacks = 0
        self.bytes_in_flight = max(0, self.bytes_in_flight - p.length)
        if self.cwin < self.ssthresh:
            self.cwin += p.length
            self.state = CCState.slow_start
        else:
            self.cwin += int(self.mss / (self.cwin // self.mss))
            self.state = CCState.congestion_avoidance

    def packet_dupack(self, time: float):
        self.dupacks += 1
        if self.dupacks >= 3:
            self.cwin //= 2
            self.ssthresh = self.cwin
            self.state = CCState.recovery

    def packet_lost(self, p: Packet, time: float):
        self.ssthresh = min(2 * self.mss, self.cwin // 2)
        self.cwin = self.mss
        self.state = CCState.recovery


@dataclass
class Path:
    name: str
    interface: Interface
    cc: CongestionController
    # Attributes for scheduling
    srtt: float = 0
    priority: int = 10

    # Private attributes
    _send_times: Dict[Packet, float] = field(default_factory=dict)

    def send(self, p: Packet, time: float) -> Union[bool, float]:
        """ Returns whether the packet was accepted or the next time at which a packet may be accepted. """
        p.sending_path = self
        sent = self.interface.send(p, time)
        if type(sent) is bool:
            if sent:
                self.cc.packet_sent(p, time)
                self._send_times[p] = time
            else:
                pass  # TODO: Too big MTU
        else:  # TODO: Packet loss always happen at sending time when the link is full, while ACKs arrive after 1 RTT
            self.cc.packet_dupack(time)
        return sent

    def receive(self, time: float) -> Union[Packet, float]:
        """ Returns the first packet received or the next time at which a packet may be received. """
        received = self.interface.receive(time)
        if type(received) is Packet and received.type == PacketType.ACK:
            acked: Packet = received.packet_acknowledged
            acked_path: Path = received.packet_acknowledged.sending_path
            acked_path._packet_acknowledged(acked, time)
        return received

    def _packet_acknowledged(self, packet: Packet, time: float):
        self.cc.packet_acknowledged(packet, time)
        if packet not in self._send_times:
            return
        sending_time = self._send_times[packet]
        rtt_estimate = time - sending_time

        if self.srtt == 0:
            self.srtt = rtt_estimate
        else:
            self.srtt = ((1 / 8) * rtt_estimate) + ((7 / 8) * self.srtt)

    def blocked(self, packet_len: int):
        """ Returns whether the congestion controller accepts the sending of a packet of length packet_len. """
        return self.cc.blocked(packet_len)

    @property
    def cc_state(self) -> CCState:
        return self.cc.state


@dataclass
class Scheduler:
    paths: List[Path]

    def schedule(self, packet_len: int) -> Optional[Path]:
        raise NotImplementedError


@dataclass
class Host:
    name: str
    paths: List[Path]
    scheduler: Scheduler
    active: bool = field(init=False, default=True)

    def run(self, time: float) -> float:
        raise NotImplementedError

    def compute_min_mss(self):
        return min(p.interface.sending_link.mtu for p in self.paths)

    def acknowledge(self, p: Packet, time: float):
        ack = Packet(type=PacketType.ACK)  # TODO: ACKs are zero length
        ack.packet_acknowledged = p
        ret = self.send(ack, time)
        if type(ret) is bool and ret:
            print()

    def send(self, p: Packet, time: float) -> Union[bool, float]:
        path = self.scheduler.schedule(p.length)
        if path:
            ret = path.send(p, time)
            if type(ret) is bool and ret:
                print(f'{time:03.03f} {self.name}:{path.name} => {p.log_str()}', end=' ')
            return ret
        return time + 1  # We don't know when a path will become ready

    def receive(self, time: float) -> Union[Packet, float]:
        next_time = float('inf')
        for p in self.paths:
            rcvd = p.receive(time)
            if type(rcvd) is Packet:
                print(f'{time:.03f} {self.name}:{p.name} <= {rcvd.log_str()}', end=' ')
                return rcvd
            elif type(rcvd) is float:
                next_time = min(rcvd, next_time)

        return next_time


@dataclass
class Client(Host):
    offset: int = 0
    blocks: List[Tuple[int, int]] = field(default_factory=list)
    fin_offset: int = 0

    def run(self, time: float) -> float:
        rcvd = self.receive(time)
        if type(rcvd) is Packet:
            if rcvd.type != PacketType.ACK:
                if rcvd.seq_number < self.offset:
                    print('Spurious retransmit')
                elif rcvd.seq_number > self.offset:
                    print('Out-of-order packet')
                    self.blocks.append((rcvd.seq_number, rcvd.length))
                    self.blocks.sort(key=lambda t: t[0])
                else:
                    self.offset += rcvd.length
                    for start, length in self.blocks[:]:
                        if self.offset == start:
                            self.offset += length
                            self.blocks = self.blocks[1:]
                        else:
                            break
                    print()
                self.acknowledge(rcvd, time)
                if rcvd.type == PacketType.FIN:
                    self.fin_offset = rcvd.seq_number
            else:
                print()
            if self.offset == self.fin_offset:
                return float('inf')
            return self.run(time)
        return max(rcvd, time)


@dataclass
class Server(Host):
    file_size: int  # bytes
    offset: int = 0  # bytes
    min_mss: int = field(init=False)
    fin_sent: bool = field(init=False)

    def __post_init__(self):
        self.min_mss = self.compute_min_mss()
        self.fin_sent = False

    def run(self, time: float) -> float:
        rcvd = self.receive(time)
        while type(rcvd) is Packet:
            print()
            if rcvd.type != PacketType.ACK:
                self.acknowledge(rcvd, time)
            elif rcvd.packet_acknowledged.type == PacketType.FIN:
                return float('inf')  # FIN ACK received, mark the simulation as complete
            rcvd = self.receive(time)

        next_time = rcvd

        if self.offset < self.file_size:  # Is there data to send ?
            packet_len = min(self.file_size, self.min_mss)
            p = Packet(length=packet_len, type=PacketType.DATA, seq_number=self.offset)
            ret = self.send(p, time)
            if type(ret) is bool:
                if ret:
                    self.offset += packet_len
                    next_time = time
                    print()
                else:
                    next_time = time + 1
            else:
                next_time = min(next_time, ret)
        elif not self.fin_sent:  # Was the FIN sent ?
            ret = self.send(Packet(length=0, type=PacketType.FIN, seq_number=self.file_size), time)
            self.fin_sent = ret is True
            if type(ret) is float:
                next_time = min(next_time, ret)
            print()

        return max(next_time, time)


@dataclass
class Simulator:
    hosts: List[Host]
    time: float = 0

    def run(self, until: float = float('inf')):
        while self.time < until:
            next_time = float('inf')
            for h in self.hosts:
                if not h.active:
                    continue
                t = h.run(self.time)
                if t == float('inf'):
                    print(h.name, 'has finished')
                    h.active = False
                next_time = min(next_time, t)
            self.time = next_time


class RoundRobin(Scheduler):
    """ Chooses an available path in a round-robin manner between multiple paths. """
    last_path: Optional[Path] = None

    def schedule(self, packet_len: int) -> Optional[Path]:
        next_idx = self.paths.index(self.last_path) + 1 if self.last_path in self.paths else 0
        sorted_paths = self.paths[next_idx:] + self.paths[:next_idx]
        for p in sorted_paths:
            if not p.blocked(packet_len):
                self.last_path = p
                return p


class WeightedRoundRobin(Scheduler):
    """ Chooses an available path in a round-robin manner following a fixed distribution. """
    distribution: List[Path]
    last_idx: int = -1

    def schedule(self, packet_len: int) -> Optional[Path]:
        next_idx = (self.last_idx + 1) % len(self.distribution)
        sorted_paths = self.distribution[next_idx:] + self.distribution[:next_idx]
        for i, p in enumerate(sorted_paths):
            if not p.blocked(packet_len):
                self.last_idx = (self.last.idx + i) % len(self.distribution)
                return p

class StrictPriority(Scheduler):
    """ Chooses the first available path in a priority list of paths. """

    def schedule(self, packet_len: int) -> Optional[Path]:
        for p in sorted(self.paths, key=lambda path: path.priority, reverse=True):
            if not p.blocked(packet_len):
                return p


@dataclass
class DelayThreshold(Scheduler):
    """ Chooses the first available path below a certain delay threshold. """
    threshold: float

    def schedule(self, packet_len: int) -> Optional[Path]:
        for p in self.paths:
            if p.srtt < self.threshold and not p.blocked(packet_len):
                return p


class LowestRTTFirst(Scheduler):
    """ Chooses the first available path with the lowest RTT. """

    def schedule(self, packet_len: int) -> Optional[Path]:
        # Sort paths by increasing SRTT
        for p in sorted(self.paths, key=lambda path: path.srtt):
            if not p.blocked(packet_len) and p.cc.state is not CCState.recovery:
                return p


if __name__ == "__main__":
    MSS = 500

    def make_link():
        return Link(bandwidth=10_000, delay=0.05, mtu=MSS)  # 10Mbps link with 50ms one-way-delay and 500 bytes MTU


    link_c1_s1 = make_link()
    link_c2_s2 = make_link()
    link_s1_c1 = make_link()
    link_s2_c2 = make_link()

    path_c1_s1 = Path(name='Path1', interface=Interface(sending_link=link_c1_s1, receiving_link=link_s1_c1), cc=NewReno(mss=MSS))
    path_c2_s2 = Path(name='Path2', interface=Interface(sending_link=link_c2_s2, receiving_link=link_s2_c2), cc=NewReno(mss=MSS))
    path_s1_c1 = Path(name='Path1', interface=Interface(sending_link=link_s1_c1, receiving_link=link_c1_s1), cc=NewReno(mss=MSS))
    path_s2_c2 = Path(name='Path2', interface=Interface(sending_link=link_s2_c2, receiving_link=link_c2_s2), cc=NewReno(mss=MSS))

    client_paths = [path_c1_s1, path_c2_s2]
    server_paths = [path_s1_c1, path_s2_c2]

    Simulator(hosts=[
        Client(name='Client', paths=client_paths, scheduler=RoundRobin(paths=client_paths)),
        Server(name='Server', paths=server_paths, scheduler=RoundRobin(paths=server_paths), file_size=10_000)
    ]).run(until=60)
