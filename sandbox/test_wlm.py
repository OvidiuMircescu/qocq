import workloadmanager
import asyncio

async def main():
  wlm = workloadmanager.WorkloadManager(4)
  commands = []
  for i in range(20):
    comm = "./toto_com.sh {} t{}".format(str( (i % 3) + 1), i)
    commands.append(wlm.run(comm))
  await asyncio.gather(*commands)

if __name__ == '__main__':
  asyncio.run(main())
  
