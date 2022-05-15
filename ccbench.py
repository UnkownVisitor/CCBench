
import json
import os
import sys
import time
import subprocess

from threading import Thread
from threading import Lock

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.link import TCLink
from mininet.clean import cleanup

MAX_HOST_NUMBER = 256
INTERVAL = 0.04
output_dir = os.path.join("result/", time.strftime("%Y%m%d%H%M%S"))
lock = Lock()


class DumbbellTopo(Topo):
    def build(self, n):
        switch1 = self.addSwitch('s1')
        switch2 = self.addSwitch('s2')
        switch3 = self.addSwitch('s3')
        switch4 = self.addSwitch('s4')

        self.addLink(switch1, switch2)
        self.addLink(switch2, switch3)
        self.addLink(switch3, switch4)

        for i in range(n):
            host = self.addHost(f"h{i}")
            self.addLink(host, switch1)
            receiver = self.addHost(f"r{i}")
            self.addLink(receiver, switch4)


def wait_and_run(delay, node, command):
    time.sleep(delay)
    print(f"{node}@{delay}s: {command}")
    lock.acquire()
    node.cmd(command)
    lock.release()


def run_test(config):
    threads = []

    duration = config["link"]["inital"]["duration"]
    host_number = len(config["connections"]["tcp"]) + \
        len(config["connections"]["udp"])

    print("Starting test...")
    print(f"Duraton: {duration}s")

    topo = DumbbellTopo(host_number)
    net = Mininet(topo, link=TCLink)
    net.start()

    subprocess.Popen(["tcpdump", "-i", "s1-eth1", "-n", "tcp", "-s", "88",
                      "-w", os.path.join(output_dir, "s1.pcap")])
    subprocess.Popen(["tcpdump", "-i", "s4-eth1", "-n", "tcp", "-s", "88",
                      "-w", os.path.join(output_dir, "s4.pcap")])

    s2, s3 = net.get("s2", "s3")
    s2.cmd("tc qdisc add dev s2-eth2 root netem delay {}ms loss {}%"
           .format(config["link"]["inital"]["rtt"],
                   config["link"]["inital"]["loss"]))
    s3.cmd("tc qdisc add dev s3-eth2 root netem rate {}mbit limit {}"
           .format(config["link"]["inital"]["bw"],
                   config["link"]["inital"]["buffer"]))
    s3.cmd("./buffer.sh {} s3-eth2 >> {} &"
           .format(INTERVAL, os.path.join(output_dir, "s3-eth2.buffer")))

    for change in config["link"]["changes"]:
        threads.append(Thread(target=wait_and_run, kwargs={
            "delay": change["start"],
            "node": s2,
            "command": "tc qdisc change dev s2-eth2 root netem delay {}ms loss {}%"
                       .format(change["rtt"],
                               change["loss"])
        }))
        threads.append(Thread(target=wait_and_run, kwargs={
            "delay": change["start"],
            "node": s3,
            "command": "tc qdisc change dev s3-eth2 root netem rate {}mbit limit {}"
                       .format(change["bw"],
                               change["buffer"])
        }))

    host_count = 0

    for tcp in config["connections"]["tcp"]:
        send = net.get(f"h{host_count}")
        recv = net.get(f"r{host_count}")
        host_count += 1

        send.setIP(f"10.1.0.{host_count}/8")
        recv.setIP(f"10.2.0.{host_count}/8")

        send.cmd(f"tc qdisc add dev {send}-eth0 root fq pacing")
        send.cmd(
            f"ip route change 10.0.0.0/8 dev {send}-eth0 congctl {tcp['cc']}")
        send.cmd(f"ethtool -K {send}-eth0 tso off")
        recv.cmd(f"timeout {duration} nc -klp 9000 > /dev/null &")

        send.cmd("./ss.sh {} >> {}.bbr &".format(INTERVAL,
                                                 os.path.join(output_dir, send.IP())))

        threads.append(Thread(target=wait_and_run, kwargs={
            "delay": tcp["start"],
            "node": send,
            "command": "timeout {} nc {} 9000 < /dev/urandom > /dev/null &"
                       .format(tcp["end"]-tcp["start"], recv.IP())
        }))

    for udp in config["connections"]["udp"]:
        send = net.get(f"h{host_count}")
        recv = net.get(f"r{host_count}")
        host_count += 1

        send.setIP(f"10.1.0.{host_count}/8")
        recv.setIP(f"10.2.0.{host_count}/8")

        send.cmd(
            f"tc qdisc add dev {send}-eth0 root netem rate {udp['bw']}mbit")
        send.cmd(f"ip route change 10.0.0.0/8 dev {send}-eth0")
        send.cmd(f"ethtool -K {send}-eth0 tso off")
        recv.cmd(f"timeout {duration} nc -klp 9000 > /dev/null &")

        threads.append(Thread(target=wait_and_run, kwargs={
            "delay": udp["start"],
            "node": send,
            "command": "timeout {} nc -u {} 9000 < /dev/urandom > /dev/null &"
                       .format(udp["end"]-udp["start"], recv.IP())
        }))

    for t in threads:
        t.start()

    time.sleep(duration)

    for t in threads:
        t.join()

    print("Test ended.")

    net.stop()
    cleanup()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 ccbench.py config")
        sys.exit()

    config_dir = str(sys.argv[1])
    if not os.path.isfile(config_dir):
        print("Error: config missing")
        sys.exit()

    json_str = open(config_dir).read()
    config = json.loads(json_str)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(os.path.join(output_dir, "config.json"), "w") as f:
        f.write(f"{config}".replace("'", '"'))

    run_test(config)
