#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by ZhangYichao on 2018/6/21


import multiprocessing

def f(q , id):
    data = q.get(block = True)
    while data != None:
        data = q.get(block = True)
        print "%d : data is %d" % (id , data)
    return

if __name__ == "__main__":
    q  = multiprocessing.Queue()
    p1 = multiprocessing.Process(target=f , args = (q,1))
    p2 = multiprocessing.Process(target=f , args = (q,2))
    for i in range(100):
        q.put(i,block = False)
    p1.start()
    p2.start()
    p1.join()
    p2.join()