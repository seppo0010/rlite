import argparse
import os
import subprocess
import sys

def find(haystack, needle):
	if needle not in haystack:
		return 0
	start = haystack.rindex(needle) + len(needle) + 1
	end = start + min(haystack[start:].find(' '), haystack[start:].find('\n'))
	return int(haystack[start:end])

def run_test(memcheck, skip):
	try:
		if memcheck:
			command = 'valgrind', '--track-origins=yes', '--leak-check=full', '--show-reachable=yes', './rlite-test'
		else:
			command = 'valgrind', '--tool=none', './rlite-test'
		p = subprocess.Popen(command + ('1', str(skip)), stderr=subprocess.PIPE)
		_, stderr = p.communicate()
	except:
		pass

	if p.returncode != 0:
		print 'Crashed on test', skip, stderr
		sys.exit(1)
	if memcheck and ('All heap blocks were freed -- no leaks are possible' not in stderr or 'Invalid rl_free' in stderr or 'Conditional jump' in stderr or 'Invalid' in stderr):
		print 'memory errors in ', skip, stderr
		sys.exit(1)
	print stderr
	return 'Simulating OOM' in stderr, find(stderr, 'passed_tests')

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Run rlite tests')
	parser.add_argument('--skip', help='Skip the first tests', default=0)
	parser.add_argument('--memcheck', action='store_true', help='Whether to use valgrind memcheck')
	args = parser.parse_args()

	skip = args.skip
	while True:
		simulated, skip = run_test(args.memcheck, skip)
		if not simulated:
			print 'Finished'
			break
		print 'ran test', skip
