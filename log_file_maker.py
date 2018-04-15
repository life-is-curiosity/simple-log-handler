import time
import asyncio 

async def write2file(file):
    with open(file, "w") as f:
        f.write("{} -- 11111111111111111".format(file))
        for i in range(0, 300000): 
            f.write("test")
            # if i % 10 == 0:
            #     f.flush()
            f.flush()
            asyncio.sleep(1)
        f.write("{} -- 22222222222222".format(file))
        f.flush()


if __name__ == '__main__':
    tasks = [
        write2file('D:\\logs\\log.log'),
        write2file('D:\\logs\\basement.log')
    ]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))