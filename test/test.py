"""
Time:     2022/6/7 19:11
Author:   LEHOSO
Version:  V 0.1
File:     test.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
import random


def random_int_list(start, stop, length):
    start, stop = (int(start), int(stop)) if start <= stop else (int(stop), int(start))
    length = int(abs(length)) if length else 0
    random_list = []
    for i in range(length):
        random_list.append(random.randint(start, stop))
    return random_list


print(random_int_list(1, 20, 10))
