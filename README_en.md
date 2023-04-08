# 0xdeadbeef

[[🇺🇸English](./README_en.md)] [[🇨🇳中文](./README.md)] 

## 0. 👋 Hi! There.

Our project focuses on exploring the area of blockchain MEV (Miner Extractable Value). We aim to develop two solutions to tackle two famous challenges respectively in this field: message latency and malicious `erc20` tokens.

Specifically, we build two program to demostrate our ideas.
- [x] 💨 Pioplat: A variation `geth` client with a network latency reduction algorithms, or peers selection stragty. What's more, we also design `relay` mechanism to further reduce latency.
- [x] 🔰 TFSniffer: A malicious tokens checker based on symbolic execution. Here we design serveral patterns to checker whether exists malicious action in `transfer` function, such as blocklist and whitelist, or fee on transfer. Thanks to the powerful symbolic execution technology, we can traverse almost all executable paths, including paths with different orders of multiple functions.

We are excited to showcase our project at the [ETHBeijing](https://www.ethbeijing.xyz/) hackathon and look forward to collaborating with other like-minded developers and innovators.

## 1. 💨 Pioplat

As decentralized applications on blockchains become more prevalent, more and more latency-sensitive use cases are emerging, with MEV being one of them. The lower the latency of sending and receiving messages, the greater the chance of earning revenue. To address this need, we have developed Pioplat - a feasible, customizable, and cost-effective latency reduction solution that uses multiple relay nodes located across different continents, along with at least one variant of a full node.

Pioplat uses a node selection strategy and a low-latency communication protocol that allows for effective latency reduction. We provided the complete implementation of Pioplat to encourage further research and enable its application in other blockchain systems. We tested this solution using five relay nodes on different continents and demonstrated its effectiveness in reducing the latency of receiving blocks/transactions, making it suitable for latency-sensitive use cases like MEV.

<img src="README.assets/pioplat-world.gif" alt="pioplat-world" width="400"/>

### 1.1 Geth variant

We have modified the network part of the `geth` client by adding a peers selection algorithm. Specifically,  at regular intervals, we score our current neighbor peers based on the order in which they send messages, and then remove nodes with low scores and connect to new nodes. For more detailed information, please refer to this [respository]().

### 1.2 Relay node

To receive information from the `p2p` network as soon as possible, an intuitive approach would be to increase the number of nodes we run. However, running multiple full nodes is prohibitively expensive due to the huge storage requirements. To address this issue, we designed stateless relay nodes that do not store any state. Each relay node is equipped with a neighbor selection strategy and queries the full nodes when a request is received from a node. Additionally, we implemented block and transaction caching mechanisms to significantly reduce the bandwidth pressure on the full nodes. For more detailed information, please refer to this [respository]().

## 2.🔰 TFSniffer



