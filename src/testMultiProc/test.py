from multiprocessing import freeze_support,Pool
import time

def Foo(i):
    time.sleep(2)
    print('___time___',time.ctime())
    return i+100

def Bar(arg):
    print('___exec done___:',arg,time.ctime())

if __name__ == '__main__':
    freeze_support()
    pool = Pool(3)

    for i in range(4):
        pool.apply_async(func=Foo,args=(i,),callback=Bar)
        # pool.apply(func=Foo,args=(i,))

    print('end')
    pool.close()
    pool.join()
    print('__main__ end')