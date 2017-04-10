import asyncio
import os
from interface import interface

def main():
	os.system('cls' if os.name == 'nt' else 'clear')
	print("Starting the interface...")

	loop = asyncio.get_event_loop()

	task = loop.create_task(interface(loop))

	loop.run_until_complete(task)
	loop.close()

if __name__ == "__main__":
	main()